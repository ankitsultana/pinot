package org.apache.pinot.common.request.context;

import java.util.concurrent.TimeUnit;
import org.apache.pinot.tsdb.spi.AggInfo;

public class TimeSeriesContext {
  private final String _timeColumn;
  private final TimeUnit _timeUnit;
  private final Long _startTs;
  private final Long _endTs;
  private final Long _offsetSeconds;
  private final Long _evaluationTimestamp;
  private final ExpressionContext _valueExpression;
  private final AggInfo _aggInfo;

  public TimeSeriesContext(String timeColumn, TimeUnit timeUnit, Long startTs, Long endTs, Long offsetSeconds, Long evaluationTimestamp, ExpressionContext valueExpression, AggInfo aggInfo) {
    _timeColumn = timeColumn;
    _timeUnit = timeUnit;
    _startTs = startTs;
    _endTs = endTs;
    _offsetSeconds = offsetSeconds;
    _evaluationTimestamp = evaluationTimestamp;
    _valueExpression = valueExpression;
    _aggInfo = aggInfo;
  }

  public String getTimeColumn() {
    return _timeColumn;
  }

  public TimeUnit getTimeUnit() {
    return _timeUnit;
  }

  public Long getStartTs() {
    return _startTs;
  }

  public Long getEndTs() {
    return _endTs;
  }

  public Long getOffsetSeconds() {
    return _offsetSeconds;
  }

  public Long getEvaluationTimestamp() {
    return _evaluationTimestamp;
  }

  public ExpressionContext getValueExpression() {
    return _valueExpression;
  }

  public AggInfo getAggInfo() {
    return _aggInfo;
  }
}
