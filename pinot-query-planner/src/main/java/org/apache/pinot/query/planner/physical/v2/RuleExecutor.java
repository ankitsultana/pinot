package org.apache.pinot.query.planner.physical.v2;

import org.apache.pinot.query.context.PhysicalPlannerContext;
import org.apache.pinot.query.planner.logical.rel2plan.PRelNode;


public abstract class RuleExecutor {
  public abstract PRelNode execute(PRelNode currentNode, PRelOptRule rule, PhysicalPlannerContext context);
}
