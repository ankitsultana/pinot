package org.apache.pinot.core.query.aggregation.function;

import java.util.List;
import java.util.Map;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.tsdb.spi.series.TimeSeries;


public class TimeSeriesAggregationFunction implements AggregationFunction<Double[], TimeSeries> {
  private final AggregationFunctionType _aggregationFunctionType;
  private final ExpressionContext _valueExpression;
  private final ExpressionContext _timeExpression;
  private final int _numTimeBuckets;

  public TimeSeriesAggregationFunction(AggregationFunctionType aggregationFunctionType,
      ExpressionContext valueExpression, ExpressionContext timeExpression, ExpressionContext numTimeBuckets) {
    _aggregationFunctionType = aggregationFunctionType;
    _valueExpression = valueExpression;
    _timeExpression = timeExpression;
    _numTimeBuckets = numTimeBuckets.getLiteral().getIntValue();
  }

  @Override
  public AggregationFunctionType getType() {
    return _aggregationFunctionType;
  }

  @Override
  public String getResultColumnName() {
    return _aggregationFunctionType.getName();
  }

  @Override
  public List<ExpressionContext> getInputExpressions() {
    return List.of(_valueExpression, _timeExpression);
  }

  @Override
  public AggregationResultHolder createAggregationResultHolder() {
    return new ObjectAggregationResultHolder();
  }

  @Override
  public GroupByResultHolder createGroupByResultHolder(int initialCapacity, int maxCapacity) {
    return new ObjectGroupByResultHolder(initialCapacity, maxCapacity);
  }

  @Override
  public void aggregate(int length, AggregationResultHolder aggregationResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    int[] timeIndexes = blockValSetMap.get(_timeExpression).getIntValuesSV();
    double[] values = blockValSetMap.get(_valueExpression).getDoubleValuesSV();
    Double[] currentValues = (Double[]) aggregationResultHolder.getResult();
    if (currentValues == null) {
      currentValues = new Double[_numTimeBuckets];
      aggregationResultHolder.setValue(currentValues);
    }
    for (int docIndex = 0; docIndex < length; docIndex++) {
      int timeIndex = timeIndexes[docIndex];
      switch (_aggregationFunctionType) {
        case TIMESERIES_MAX:
          currentValues[timeIndex] = Math.max(currentValues[timeIndex], values[timeIndex]);
          break;
        case TIMESERIES_MIN:
          currentValues[timeIndex] = Math.min(currentValues[timeIndex], values[timeIndex]);
          break;
        case TIMESERIES_SUM:
          currentValues[timeIndex] = currentValues[timeIndex] + values[timeIndex];
          break;
        default:
          throw new UnsupportedOperationException("Unknown aggregation function: " + _aggregationFunctionType);
      }
    }
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    // get time index: from blockValSetMap
    // get values:     from blockValSetMap
    int[] timeIndexes = blockValSetMap.get(_timeExpression).getIntValuesSV();
    double[] values = blockValSetMap.get(_valueExpression).getDoubleValuesSV();
    for (int docIndex = 0; docIndex < groupKeyArray.length; docIndex++) {
      int groupId = groupKeyArray[docIndex];
      int timeIndex = timeIndexes[docIndex];
      Double[] currentValues = groupByResultHolder.getResult(groupId);
      if (currentValues == null) {
        currentValues = new Double[_numTimeBuckets];
        groupByResultHolder.setValueForKey(groupId, currentValues);
      }
      switch (_aggregationFunctionType) {
        case TIMESERIES_MAX:
          currentValues[timeIndex] = Math.max(currentValues[timeIndex], values[timeIndex]);
          break;
        case TIMESERIES_MIN:
          currentValues[timeIndex] = Math.min(currentValues[timeIndex], values[timeIndex]);
          break;
        case TIMESERIES_SUM:
          currentValues[timeIndex] = currentValues[timeIndex] + values[timeIndex];
          break;
        default:
          throw new UnsupportedOperationException("Unknown aggregation function: " + _aggregationFunctionType);
      }
    }
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    throw new UnsupportedOperationException("");
  }

  @Override
  public Double[] extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    return aggregationResultHolder.getResult();
  }

  @Override
  public Double[] extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    return groupByResultHolder.getResult(groupKey);
  }

  @Override
  public Double[] merge(Double[] intermediateResult1,
      Double[] intermediateResult2) {
    switch (_aggregationFunctionType) {
      case TIMESERIES_MAX:
        mergeMax(intermediateResult1, intermediateResult2);
        break;
      case TIMESERIES_MIN:
        mergeMin(intermediateResult1, intermediateResult2);
        break;
      case TIMESERIES_SUM:
        mergeSum(intermediateResult1, intermediateResult2);
        break;
      default:
        throw new UnsupportedOperationException("Found unsupported time series aggregation: " + _aggregationFunctionType);
    }
    return intermediateResult1;
  }

  @Override
  public DataSchema.ColumnDataType getIntermediateResultColumnType() {
    return DataSchema.ColumnDataType.DOUBLE_ARRAY;
  }

  @Override
  public DataSchema.ColumnDataType getFinalResultColumnType() {
    return DataSchema.ColumnDataType.DOUBLE_ARRAY;
  }

  @Override
  public TimeSeries extractFinalResult(Double[] seriesBuilder) {
    throw new UnsupportedOperationException("Extract final result not supported");
  }

  @Override
  public String toExplainString() {
    return "TIME_SERIES";
  }

  private void mergeSum(Double[] ir1, Double[] ir2) {
    for (int index = 0; index < _numTimeBuckets; index++) {
      if (ir2[index] != null) {
        ir1[index] = ir1[index] == null ? ir2[index] : (ir1[index] + ir2[index]);
      }
    }
  }

  private void mergeMin(Double[] ir1, Double[] ir2) {
    for (int index = 0; index < _numTimeBuckets; index++) {
      if (ir2[index] != null) {
        ir1[index] = ir1[index] == null ? ir2[index] : Math.min(ir1[index], ir2[index]);
      }
    }
  }

  private void mergeMax(Double[] ir1, Double[] ir2) {
    for (int index = 0; index < _numTimeBuckets; index++) {
      if (ir2[index] != null) {
        ir1[index] = ir1[index] == null ? ir2[index] : Math.max(ir1[index], ir2[index]);
      }
    }
  }
}
