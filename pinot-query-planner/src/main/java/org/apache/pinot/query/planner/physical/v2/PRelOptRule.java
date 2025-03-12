package org.apache.pinot.query.planner.physical.v2;

import org.apache.pinot.query.planner.logical.rel2plan.PRelNode;


/**
 * Optimization rule for {@link PRelNode}.
 */
public abstract class PRelOptRule {
  public boolean matches(PRelOptRuleCall call) {
    return true;
  }

  public abstract PRelNode onMatch(PRelOptRuleCall call);

  public PRelNode onDone(PRelNode currentNode) {
    return currentNode;
  }
}
