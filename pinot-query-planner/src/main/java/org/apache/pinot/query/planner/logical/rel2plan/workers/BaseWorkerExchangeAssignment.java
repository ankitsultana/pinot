package org.apache.pinot.query.planner.logical.rel2plan.workers;

import org.apache.pinot.query.planner.logical.rel2plan.PRelNode;


public abstract class BaseWorkerExchangeAssignment {
  public abstract PRelNode assign(PRelNode rootNode);
}
