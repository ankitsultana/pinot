package org.apache.pinot.tsdb.spi.plan.visitor;

import org.apache.pinot.tsdb.spi.plan.BaseTimeSeriesPlanNode;
import org.apache.pinot.tsdb.spi.plan.ExchangePlanNode;
import org.apache.pinot.tsdb.spi.plan.ScanFilterAndProjectPlanNode;


public abstract class PlanVisitor<T> {
  public abstract T visit(BaseTimeSeriesPlanNode node);

  public abstract T visit(ScanFilterAndProjectPlanNode node);

  public abstract T visit(ExchangePlanNode node);
}
