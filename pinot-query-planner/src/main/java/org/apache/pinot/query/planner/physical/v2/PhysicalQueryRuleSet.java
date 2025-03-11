package org.apache.pinot.query.planner.physical.v2;

import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.query.context.PhysicalPlannerContext;
import org.apache.pinot.query.planner.physical.v2.rules.GeneralWorkerAssignmentRule;
import org.apache.pinot.query.planner.physical.v2.rules.LeafStageBoundaryRule;
import org.apache.pinot.query.planner.physical.v2.rules.SortPushdownRule;
import org.apache.pinot.query.planner.physical.v2.rules.TableScanWorkerAssignmentRule;


public class PhysicalQueryRuleSet {
  private PhysicalQueryRuleSet() {
  }

  public static List<Pair<PRelOptRule, RuleExecutor>> create(PhysicalPlannerContext context,
      Map<String, String> queryOptions) {
    return List.of(
        Pair.of(LeafStageBoundaryRule.INSTANCE, RuleExecutors.POST_ORDER),
        Pair.of(new TableScanWorkerAssignmentRule(context), RuleExecutors.POST_ORDER),
        Pair.of(GeneralWorkerAssignmentRule.INSTANCE, RuleExecutors.IN_ORDER),
        Pair.of(SortPushdownRule.INSTANCE, RuleExecutors.POST_ORDER));
  }
}
