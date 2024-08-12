package org.apache.pinot.query.runtime.timeseries;

import java.util.List;
import java.util.Map;
import org.apache.pinot.tsdb.spi.TimeBuckets;


public class TimeSeriesExecutionContext {
  private final String _engine;
  private final TimeBuckets _initialTimeBuckets;
  private final Map<String, List<String>> _planIdToSegmentsMap;

  public TimeSeriesExecutionContext(String engine, TimeBuckets initialTimeBuckets,
      Map<String, List<String>> planIdToSegmentsMap) {
    _engine = engine;
    _initialTimeBuckets = initialTimeBuckets;
    _planIdToSegmentsMap = planIdToSegmentsMap;
  }

  public String getEngine() {
    return _engine;
  }

  public TimeBuckets getInitialTimeBuckets() {
    return _initialTimeBuckets;
  }

  public Map<String, List<String>> getPlanIdToSegmentsMap() {
    return _planIdToSegmentsMap;
  }
}
