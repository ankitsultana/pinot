package org.apache.pinot.tsdb.spi;

import java.util.Map;


public interface TimeSeriesLogicalPlanner {
  void init(Map<String, Object> config);

  TimeSeriesLogicalPlanResult plan(RangeTimeSeriesRequest request);
}
