package org.apache.pinot.query.planner.physical.v2.rules;

import org.apache.calcite.rel.core.TableScan;
import org.apache.pinot.query.planner.logical.rel2plan.PRelNode;
import org.apache.pinot.query.planner.physical.v2.PRelOptRule;
import org.apache.pinot.query.planner.physical.v2.PRelOptRuleCall;


public class TableScanWorkerAssignmentRule extends PRelOptRule {
  public static final TableScanWorkerAssignmentRule INSTANCE = new TableScanWorkerAssignmentRule();

  private TableScanWorkerAssignmentRule() {
  }

  @Override
  public boolean matches(PRelOptRuleCall call) {
    return call._currentNode.getRelNode() instanceof TableScan;
  }

  @Override
  public PRelNode onMatch(PRelOptRuleCall call) {
    return null;
  }
}
