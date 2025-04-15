package org.apache.pinot.query.planner.physical.v2.opt.rules;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.pinot.query.context.PhysicalPlannerContext;
import org.apache.pinot.query.planner.logical.RexExpressionUtils;
import org.apache.pinot.query.planner.physical.v2.PRelNode;
import org.apache.pinot.query.planner.physical.v2.nodes.PhysicalExchange;
import org.apache.pinot.query.planner.physical.v2.nodes.PhysicalSort;
import org.apache.pinot.query.planner.physical.v2.opt.PRelOptRule;
import org.apache.pinot.query.planner.physical.v2.opt.PRelOptRuleCall;
import org.apache.pinot.query.type.TypeFactory;


/**
 * <h1>Overview</h1>
 * When a Sort node is on top of an Exchange, it may make sense to add a copy of the Sort under the Exchange too for
 * performance reasons. E.g. if the Sort has a fetch of 50 rows, then it makes sense to trim the amount of rows
 * sent across the Exchange too.
 * <h1>Handling Offsets</h1>
 */
public class SortPushdownRule extends PRelOptRule {
  private static final int DEFAULT_SORT_EXCHANGE_COPY_THRESHOLD = 10_000;
  private static final TypeFactory TYPE_FACTORY = new TypeFactory();
  private static final RexBuilder REX_BUILDER = new RexBuilder(TYPE_FACTORY);
  private static final RexLiteral REX_ZERO = REX_BUILDER.makeLiteral(0,
      TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER));
  private final PhysicalPlannerContext _context;

  public SortPushdownRule(PhysicalPlannerContext context) {
    _context = context;
  }

  @Override
  public boolean matches(PRelOptRuleCall call) {
    return call._currentNode.unwrap() instanceof Sort
        && call._currentNode.unwrap().getInput(0) instanceof Exchange;
  }

  @Override
  public PRelNode onMatch(PRelOptRuleCall call) {
    // TODO: Make it as smart as existing PinotSortExchangeCopyRule, which tracks max amount of rows. However, the
    //   existing rule leverages RelMetadataQuery.
    // TODO: Some other condition checking is missing here like comparing collations.
    PhysicalSort sort = (PhysicalSort) call._currentNode.unwrap();
    final RexNode fetch;
    if (sort.fetch == null) {
      fetch = null;
    } else if (sort.offset == null) {
      fetch = sort.fetch;
    } else {
      int total = RexExpressionUtils.getValueAsInt(sort.fetch) + RexExpressionUtils.getValueAsInt(sort.offset);
      fetch = REX_BUILDER.makeLiteral(total, TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER));
    }
    if (fetch == null) {
      return call._currentNode;
    }
    final Exchange exchange = (Exchange) sort.getInput();
    final RelNode newExchangeInput = sort.copy(sort.getTraitSet(), exchange.getInput(), sort.collation, null, fetch);
    final RelNode newExchange = exchange.copy(exchange.getTraitSet(), ImmutableList.of(newExchangeInput));
    final RelNode newTopLevelSort = sort.copy(
        sort.getTraitSet(), newExchange, sort.collation, sort.offset == null ? REX_ZERO : sort.offset, sort.fetch);
    // Old: Sort (o0) > Exchange (o1) > Input (o2)
    // New: Sort (n0) > Exchange (n1) > Sort (n2) > Input (o2)
    PRelNode o0 = call._currentNode;
    PhysicalExchange o1 = (PhysicalExchange) o0.getPRelInput(0);
    PRelNode o2 = o1.getPRelInput(0);
    PhysicalSort n2 = new PhysicalSort(sort.getCluster(), RelTraitSet.createEmpty(), List.of(), sort.collation,
        null /* offset */, fetch, o2, nodeId(), o2.getPinotDataDistributionOrThrow(), o2.isLeafStage());
    PhysicalExchange n1 = new PhysicalExchange(nodeId(), n2, o1.getPinotDataDistributionOrThrow(),
        o1.getDistributionKeys(), o1.getExchangeStrategy(), o1.getRelCollation(), o1.getExecStrategy());
    PhysicalSort n0 = new PhysicalSort(sort.getCluster(), sort.getTraitSet(), sort.getHints(), sort.getCollation(),
        sort.offset, sort.fetch, n1, sort.getNodeId(), sort.getPinotDataDistributionOrThrow(), false);
    return n0;
  }

  private int nodeId() {
    return _context.getNodeIdGenerator().get();
  }
}
