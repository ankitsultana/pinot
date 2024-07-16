package org.apache.pinot.tsdb.spi;


public interface TimeSeriesLogicalPlanner {
  TimeSeriesLogicalPlanResult plan(RangeTimeSeriesRequest request);
}
