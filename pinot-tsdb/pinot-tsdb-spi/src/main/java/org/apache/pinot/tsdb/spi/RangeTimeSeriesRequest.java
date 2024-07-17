package org.apache.pinot.tsdb.spi;

import java.time.Duration;


public class RangeTimeSeriesRequest {
  private final String _engine;
  private final String _query;
  private final long _startTs;
  private final long _endTs;
  private final long _stepSeconds;
  private final Duration _timeout;

  public RangeTimeSeriesRequest(String engine, String query, long startTs, long endTs, long stepSeconds,
      Duration timeout) {
    _engine = engine;
    _query = query;
    _startTs = startTs;
    _endTs = endTs;
    _stepSeconds = stepSeconds;
    _timeout = timeout;
  }

  public String getEngine() {
    return _engine;
  }

  public String getQuery() {
    return _query;
  }

  public long getStartTs() {
    return _startTs;
  }

  public long getEndTs() {
    return _endTs;
  }

  public long getStepSeconds() {
    return _stepSeconds;
  }

  public Duration getTimeout() {
    return _timeout;
  }
}
