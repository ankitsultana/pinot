package org.apache.pinot.query.planner.physical.v2.rules;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.pinot.query.planner.logical.rel2plan.PRelNode;
import org.apache.pinot.query.planner.physical.v2.PRelOptRule;
import org.apache.pinot.query.planner.physical.v2.PRelOptRuleCall;


public class LeafStageBoundaryRule extends PRelOptRule {
  public static final LeafStageBoundaryRule INSTANCE = new LeafStageBoundaryRule();

  private LeafStageBoundaryRule() {
  }

  @Override
  public boolean matches(PRelOptRuleCall call) {
    RelNode currentRel = call._currentNode.getRelNode();
    if (currentRel instanceof TableScan) {
      return true;
    }
    if (!(currentRel instanceof Project) && !(currentRel instanceof Filter)) {
      return false;
    }
    if (currentRel.getInput(0) instanceof TableScan) {
      return true;
    }
    return currentRel.getInput(0).getInput(0) instanceof TableScan;
  }

  @Override
  public PRelNode onMatch(PRelOptRuleCall call) {
    PRelNode currentNode = call._currentNode;
    return new PRelNode(currentNode.getNodeId(), currentNode.getRelNode(), currentNode.getPinotDataDistribution(),
        currentNode.getInputs(), true, currentNode.isLeafStageBoundary());
  }
}
