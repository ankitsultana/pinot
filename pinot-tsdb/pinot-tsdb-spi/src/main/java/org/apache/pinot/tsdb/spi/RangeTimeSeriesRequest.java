package org.apache.pinot.tsdb.spi;

import java.time.Duration;


/**
 * <ul>
 *   <li>[start, end] are both inclusive.</li>
 *   <li>The result should contain time values that are in [start, end]</li>
 *   <li>
 *     stepSeconds is used to define the default resolution for the query.
 *  </li>
 *  <li>
 *     Some query languages allow users to change the resolution via a function, and in those cases the returned
 *     time-series may have a resolution different than stepSeconds
 *  </li>
 *  <li>
 *    The query execution may scan and process data outside of the time-range [start, end]. The actual data scanned
 *    and processed is defined by the {@link TimeBuckets} used by the operator.
 *  </li>
 * </ul>
 */
public class RangeTimeSeriesRequest {
  private final String _engine;
  private final String _query;
  private final long _startSeconds;
  private final long _endSeconds;
  private final long _stepSeconds;
  private final Duration _timeout;

  public RangeTimeSeriesRequest(String engine, String query, long startSeconds, long endSeconds, long stepSeconds,
      Duration timeout) {
    _engine = engine;
    _query = query;
    _startSeconds = startSeconds;
    _endSeconds = endSeconds;
    _stepSeconds = stepSeconds;
    _timeout = timeout;
  }

  public String getEngine() {
    return _engine;
  }

  public String getQuery() {
    return _query;
  }

  public long getStartSeconds() {
    return _startSeconds;
  }

  public long getEndSeconds() {
    return _endSeconds;
  }

  public long getStepSeconds() {
    return _stepSeconds;
  }

  public Duration getTimeout() {
    return _timeout;
  }
}
