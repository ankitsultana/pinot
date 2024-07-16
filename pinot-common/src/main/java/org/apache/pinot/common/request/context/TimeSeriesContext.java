package org.apache.pinot.common.request.context;

import org.apache.pinot.tsdb.spi.AggType;

public class TimeSeriesContext {
  private final String _timeColumn;
  private final ExpressionContext _valueExpression;
  private final AggType _aggType;
  private final Long _offsetSeconds;
  private final Long _evaluationTimestamp;

  public TimeSeriesContext(String timeColumn, ExpressionContext valueExpression, AggType aggType,
      Long offsetSeconds, Long evaluationTimestamp) {
    _timeColumn = timeColumn;
    _valueExpression = valueExpression;
    _aggType = aggType;
    _offsetSeconds = offsetSeconds;
    _evaluationTimestamp = evaluationTimestamp;
  }

  public String getTimeColumn() {
    return _timeColumn;
  }

  public ExpressionContext getValueExpression() {
    return _valueExpression;
  }

  public AggType getAggType() {
    return _aggType;
  }

  public Long getOffsetSeconds() {
    return _offsetSeconds;
  }

  public Long getEvaluationTimestamp() {
    return _evaluationTimestamp;
  }
}
