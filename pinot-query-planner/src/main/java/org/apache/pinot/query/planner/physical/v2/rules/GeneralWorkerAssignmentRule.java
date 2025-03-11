package org.apache.pinot.query.planner.physical.v2.rules;

import org.apache.pinot.query.planner.logical.rel2plan.PRelNode;
import org.apache.pinot.query.planner.physical.v2.PRelOptRule;
import org.apache.pinot.query.planner.physical.v2.PRelOptRuleCall;


public class GeneralWorkerAssignmentRule extends PRelOptRule {
  public static final GeneralWorkerAssignmentRule INSTANCE = new GeneralWorkerAssignmentRule();

  @Override
  public boolean matches(PRelOptRuleCall call) {
    return !call._currentNode.hasPinotDataDistribution();
  }

  @Override
  public PRelNode onMatch(PRelOptRuleCall call) {
    PRelNode currentNode = call._currentNode;
    return null;
  }
}
