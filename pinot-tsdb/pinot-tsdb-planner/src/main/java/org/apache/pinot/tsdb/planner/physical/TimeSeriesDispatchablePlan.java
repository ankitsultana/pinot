package org.apache.pinot.tsdb.planner.physical;

import java.util.List;
import java.util.Map;
import org.apache.pinot.tsdb.spi.TimeBuckets;


public class TimeSeriesDispatchablePlan {
  private final TimeSeriesQueryServerInstance _queryServerInstance;
  private final String _engine;
  private final String _serializedPlan;
  private final TimeBuckets _timeBuckets;
  private final Map<String, List<String>> _planIdToSegments;

  public TimeSeriesDispatchablePlan(String engine, TimeSeriesQueryServerInstance queryServerInstance,
      String serializedPlan, TimeBuckets timeBuckets, Map<String, List<String>> planIdToSegments) {
    _engine = engine;
    _queryServerInstance = queryServerInstance;
    _serializedPlan = serializedPlan;
    _timeBuckets = timeBuckets;
    _planIdToSegments = planIdToSegments;
  }

  public String getEngine() {
    return _engine;
  }

  public TimeSeriesQueryServerInstance getQueryServerInstance() {
    return _queryServerInstance;
  }

  public String getSerializedPlan() {
    return _serializedPlan;
  }

  public TimeBuckets getTimeBuckets() {
    return _timeBuckets;
  }

  public Map<String, List<String>> getPlanIdToSegments() {
    return _planIdToSegments;
  }
}
