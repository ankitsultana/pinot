package org.apache.pinot.query.planner.logical.rel2plan.workers;

import org.apache.pinot.query.planner.logical.rel2plan.WrappedRelNode;


public abstract class BaseWorkerExchangeAssignment {
  public abstract WrappedRelNode assign(WrappedRelNode rootNode);
}
