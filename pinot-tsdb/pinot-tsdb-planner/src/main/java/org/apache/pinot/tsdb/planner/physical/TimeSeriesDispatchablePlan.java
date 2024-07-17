package org.apache.pinot.tsdb.planner.physical;

import java.util.List;


public class TimeSeriesDispatchablePlan {
  private final TimeSeriesQueryServerInstance _queryServerInstance;
  private final String _serializedPlan;
  private final List<String> _segments;

  public TimeSeriesDispatchablePlan(TimeSeriesQueryServerInstance queryServerInstance, String serializedPlan,
      List<String> segments) {
    _queryServerInstance = queryServerInstance;
    _serializedPlan = serializedPlan;
    _segments = segments;
  }

  public TimeSeriesQueryServerInstance getQueryServerInstance() {
    return _queryServerInstance;
  }

  public String getSerializedPlan() {
    return _serializedPlan;
  }

  public List<String> getSegments() {
    return _segments;
  }
}
