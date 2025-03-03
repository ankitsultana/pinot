package org.apache.pinot.query.planner.logical.rel2plan.workers;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.pinot.calcite.rel.PinotDataDistribution;
import org.apache.pinot.calcite.rel.PinotExchangeDesc;
import org.apache.pinot.calcite.rel.logical.PinotPhysicalExchange;
import org.apache.pinot.query.planner.logical.rel2plan.PRelNode;
import org.apache.pinot.query.planner.logical.rel2plan.PlanIdGenerator;
import org.apache.pinot.query.type.TypeFactory;


public class LiteModeWorkerExchangeAssignment extends BaseWorkerExchangeAssignment {
  private static final TypeFactory TYPE_FACTORY = new TypeFactory();
  private static final RexBuilder REX_BUILDER = new RexBuilder(TYPE_FACTORY);
  private static final int DEFAULT_FETCH_LIMIT = 100_000;
  private final PlanIdGenerator _idGenerator;
  private final RexLiteral _fetchLiteral;
  private final List<String> _randomWorker = new ArrayList<>();

  public LiteModeWorkerExchangeAssignment(PlanIdGenerator planIdGenerator) {
    _idGenerator = planIdGenerator;
    _fetchLiteral = REX_BUILDER.makeLiteral(1, TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER));
  }

  @Override
  public PRelNode assign(PRelNode rootNode) {
    // leaf stage boundary should be sort, otherwise insert sort.
    rootNode = insertLogicalSort(rootNode);
    _randomWorker.add(Objects.requireNonNull(pickRandomWorker(rootNode), "Unable to find any worker in leaf stage"));
    return assignInternal(rootNode);
  }

  private PRelNode assignInternal(PRelNode currentNode) {
    if (currentNode.isLeafStage()) {
      if (currentNode.isLeafStageBoundary()) {
        // insert singleton exchange and use _randomWorker
        Sort currentSort = (Sort) currentNode.getRelNode();
        List<Integer> keys;
        PinotPhysicalExchange pinotPhysicalExchange;
        if (currentSort.getCollation() != null && currentSort.getCollation() != RelCollations.EMPTY) {
          keys = currentSort.getCollation().getKeys();
          pinotPhysicalExchange = new PinotPhysicalExchange(currentNode.getRelNode(), keys,
              PinotExchangeDesc.SINGLETON_EXCHANGE, null);
        } else {
          pinotPhysicalExchange = new PinotPhysicalExchange(currentNode.getRelNode(), Collections.emptyList(),
              PinotExchangeDesc.SINGLETON_EXCHANGE);
        }
        PinotDataDistribution newPinotDataDistribution = new PinotDataDistribution(PinotDataDistribution.Type.SINGLETON,
            _randomWorker, _randomWorker.hashCode(), null, null);
        PRelNode newPRelNode = new PRelNode(_idGenerator.get(), pinotPhysicalExchange, newPinotDataDistribution);
        newPRelNode.addInput(currentNode);
        return newPRelNode;
      }
      return currentNode;
    }
    List<PRelNode> newInputs = new ArrayList<>();
    for (PRelNode input : currentNode.getInputs()) {
      newInputs.add(assignInternal(input));
    }
    PinotDataDistribution pinotDataDistribution = new PinotDataDistribution(PinotDataDistribution.Type.SINGLETON,
        _randomWorker, _randomWorker.hashCode(), null, null);
    return currentNode.copy(currentNode.getNodeId(), newInputs, pinotDataDistribution);
  }

  private PRelNode insertLogicalSort(PRelNode currentNode) {
    if (currentNode.isLeafStage() && currentNode.isLeafStageBoundary()) {
      if (currentNode.getRelNode() instanceof Sort) {
        Sort currentSort = (Sort) currentNode.getRelNode();
        if (currentSort.fetch != null) {
          int currentLimit = Integer.parseInt(((RexLiteral) currentSort.fetch).getValue().toString());
          if (currentLimit <= DEFAULT_FETCH_LIMIT) {
            return currentNode;
          }
        }
        // replace current sort with one that has the appropriate limit.
        Sort newSort = currentSort.copy(currentSort.getTraitSet(), currentSort.getInput(), currentSort.getCollation(),
            currentSort.offset, _fetchLiteral);
        return new PRelNode(currentNode.getNodeId(), newSort, currentNode.getPinotDataDistribution().get());
      }
      Sort sort = LogicalSort.create(currentNode.getRelNode(), RelCollations.EMPTY, null, _fetchLiteral);
      return new PRelNode(_idGenerator.get(), sort, currentNode.getPinotDataDistribution().get());
    }
    List<PRelNode> newInputs = new ArrayList<>();
    for (PRelNode input : currentNode.getInputs()) {
      newInputs.add(insertLogicalSort(input));
    }
    return currentNode.copy(_idGenerator.get(), newInputs, currentNode.getPinotDataDistribution().get());
  }

  @Nullable
  private String pickRandomWorker(PRelNode rootNode) {
    if (rootNode.getPinotDataDistribution().isPresent()) {
      return rootNode.getPinotDataDistribution().get().getWorkers().get(0);
    }
    String result = null;
    for (PRelNode input : rootNode.getInputs()) {
      result = pickRandomWorker(input);
      if (result != null) {
        return result;
      }
    }
    return result;
  }
}
