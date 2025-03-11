package org.apache.pinot.query.planner.physical.v2;

import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.query.planner.physical.v2.rules.LeafStageBoundaryRule;
import org.apache.pinot.query.planner.physical.v2.rules.TableScanWorkerAssignmentRule;


public class PhysicalQueryRuleSet {
  private PhysicalQueryRuleSet() {
  }

  public static List<Pair<PRelOptRule, RuleExecutor>> create(Map<String, String> queryOptions) {
    return List.of(
        Pair.of(LeafStageBoundaryRule.INSTANCE, RuleExecutors.POST_ORDER),
        Pair.of(TableScanWorkerAssignmentRule.INSTANCE, RuleExecutors.POST_ORDER));
  }
}
