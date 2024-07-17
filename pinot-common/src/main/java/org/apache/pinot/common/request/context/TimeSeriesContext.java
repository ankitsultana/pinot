package org.apache.pinot.common.request.context;

import org.apache.pinot.tsdb.spi.AggInfo;

public class TimeSeriesContext {
  private final String _timeColumn;
  private final ExpressionContext _valueExpression;
  private final AggInfo _aggInfo;
  private final Long _offsetSeconds;
  private final Long _evaluationTimestamp;

  public TimeSeriesContext(String timeColumn, ExpressionContext valueExpression, AggInfo aggInfo,
      Long offsetSeconds, Long evaluationTimestamp) {
    _timeColumn = timeColumn;
    _valueExpression = valueExpression;
    _aggInfo = aggInfo;
    _offsetSeconds = offsetSeconds;
    _evaluationTimestamp = evaluationTimestamp;
  }

  public String getTimeColumn() {
    return _timeColumn;
  }

  public ExpressionContext getValueExpression() {
    return _valueExpression;
  }

  public AggInfo getAggInfo() {
    return _aggInfo;
  }

  public Long getOffsetSeconds() {
    return _offsetSeconds;
  }

  public Long getEvaluationTimestamp() {
    return _evaluationTimestamp;
  }
}
