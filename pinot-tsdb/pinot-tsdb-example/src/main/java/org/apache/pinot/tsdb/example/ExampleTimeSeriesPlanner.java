package org.apache.pinot.tsdb.example;

import java.util.Map;
import org.apache.pinot.tsdb.spi.RangeTimeSeriesRequest;
import org.apache.pinot.tsdb.spi.TimeSeriesLogicalPlanResult;
import org.apache.pinot.tsdb.spi.TimeSeriesLogicalPlanner;


public class ExampleTimeSeriesPlanner implements TimeSeriesLogicalPlanner {
  @Override
  public void init(Map<String, Object> config) {
  }

  @Override
  public TimeSeriesLogicalPlanResult plan(RangeTimeSeriesRequest request) {
    if (!request.getEngine().equals(Constants.ENGINE_ID)) {
      throw new IllegalArgumentException(String.format("Invalid engine id: %s. Expected: %s", request.getEngine(),
          Constants.ENGINE_ID));
    }
    return null;
  }
}
