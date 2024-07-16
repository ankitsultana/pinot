package org.apache.pinot.tsdb.spi;

import org.apache.pinot.tsdb.spi.plan.BaseTimeSeriesPlanNode;


public class TimeSeriesLogicalPlanResult {
  private final BaseTimeSeriesPlanNode _planNode;
  private final TimeBuckets _timeBuckets;

  public TimeSeriesLogicalPlanResult(BaseTimeSeriesPlanNode planNode, TimeBuckets timeBuckets) {
    _planNode = planNode;
    _timeBuckets = timeBuckets;
  }

  public BaseTimeSeriesPlanNode getPlanNode() {
    return _planNode;
  }

  public TimeBuckets getTimeBuckets() {
    return _timeBuckets;
  }
}
