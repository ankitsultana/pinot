package org.apache.pinot.common.request.context;

import java.util.concurrent.TimeUnit;
import org.apache.pinot.tsdb.spi.AggInfo;
import org.apache.pinot.tsdb.spi.TimeBuckets;


public class TimeSeriesContext {
  private final String _engine;
  private final String _timeColumn;
  private final TimeUnit _timeUnit;
  private final TimeBuckets _timeBuckets;
  private final Long _offsetSeconds;
  private final ExpressionContext _valueExpression;
  private final AggInfo _aggInfo;

  public TimeSeriesContext(String engine, String timeColumn, TimeUnit timeUnit, TimeBuckets timeBuckets,
      Long offsetSeconds, ExpressionContext valueExpression, AggInfo aggInfo) {
    _engine = engine;
    _timeColumn = timeColumn;
    _timeUnit = timeUnit;
    _timeBuckets = timeBuckets;
    _offsetSeconds = offsetSeconds;
    _valueExpression = valueExpression;
    _aggInfo = aggInfo;
  }

  public String getEngine() {
    return _engine;
  }

  public String getTimeColumn() {
    return _timeColumn;
  }

  public TimeUnit getTimeUnit() {
    return _timeUnit;
  }

  public TimeBuckets getTimeBuckets() {
    return _timeBuckets;
  }

  public Long getOffsetSeconds() {
    return _offsetSeconds;
  }

  public ExpressionContext getValueExpression() {
    return _valueExpression;
  }

  public AggInfo getAggInfo() {
    return _aggInfo;
  }
}
