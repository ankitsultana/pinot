package org.apache.pinot.query.planner.physical.v2.rules;

import org.apache.pinot.query.planner.logical.rel2plan.PRelNode;
import org.apache.pinot.query.planner.physical.v2.PRelOptRule;
import org.apache.pinot.query.planner.physical.v2.PRelOptRuleCall;


public class SortAggregatePushdownRule extends PRelOptRule {
  @Override
  public PRelNode onMatch(PRelOptRuleCall call) {
    return null;
  }
}
