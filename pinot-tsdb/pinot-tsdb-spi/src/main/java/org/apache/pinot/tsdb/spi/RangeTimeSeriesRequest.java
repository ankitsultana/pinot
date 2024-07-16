package org.apache.pinot.tsdb.spi;

public class RangeTimeSeriesRequest {
  private final String _query;
  private final long _startTs;
  private final long _endTs;
  private final String _step;
  private final String _timeout;

  public RangeTimeSeriesRequest(String query, long startTs, long endTs, String step, String timeout) {
    _query = query;
    _startTs = startTs;
    _endTs = endTs;
    _step = step;
    _timeout = timeout;
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

  public String getStep() {
    return _step;
  }

  public String getTimeout() {
    return _timeout;
  }
}
