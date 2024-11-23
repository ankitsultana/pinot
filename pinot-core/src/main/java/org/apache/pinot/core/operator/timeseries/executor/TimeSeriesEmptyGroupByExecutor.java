package org.apache.pinot.core.operator.timeseries.executor;

import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.operator.blocks.TimeSeriesBuilderBlock;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.tsdb.spi.TimeBuckets;
import org.apache.pinot.tsdb.spi.series.BaseTimeSeriesBuilder;


public class TimeSeriesEmptyGroupByExecutor implements TimeSeriesGroupByExecutor {
  private static final int THRESHOLD_FOR_LITERAL_OPT = 1000;
  private final TimeBuckets _timeBuckets;
  private final BaseTimeSeriesBuilder _emptyGroupBySeriesBuilder;
  private final Double _literalValue;
  private final ExpressionContext _valueExpression;

  public TimeSeriesEmptyGroupByExecutor(BaseTimeSeriesBuilder seriesBuilder, TimeBuckets timeBuckets,
      ExpressionContext valueExpression) {
    _emptyGroupBySeriesBuilder = seriesBuilder;
    _timeBuckets = timeBuckets;
    _literalValue = (valueExpression.getType() == ExpressionContext.Type.LITERAL)
        ? valueExpression.getLiteral().getDoubleValue() : null;
    _valueExpression = valueExpression;
  }

  @Override
  public void process(int numDocs, int[] timeValueIndexes, ValueBlock valueBlock) {
    if (_literalValue != null) {
      processLiteral(numDocs, timeValueIndexes);
      return;
    }
    BlockValSet blockValSet = valueBlock.getBlockValueSet(_valueExpression);
    switch (blockValSet.getValueType()) {
      case LONG:
        processLongExpression(numDocs, blockValSet, timeValueIndexes);
        break;
      case INT:
        processIntExpression(numDocs, blockValSet, timeValueIndexes);
        break;
      case DOUBLE:
        processDoubleExpression(numDocs, blockValSet, timeValueIndexes);
        break;
      case STRING:
        processStringExpression(numDocs, blockValSet, timeValueIndexes);
        break;
      default:
        throw new UnsupportedOperationException(String.format("Don't support value-type: %s yet",
            blockValSet.getValueType()));
    }
  }

  @Override
  public TimeSeriesBuilderBlock getTimeSeriesBuilderBlock() {
    Map<Long, BaseTimeSeriesBuilder> seriesBuilderMap = new HashMap<>(1);
    seriesBuilderMap.put(Long.parseLong(_emptyGroupBySeriesBuilder.getId()), _emptyGroupBySeriesBuilder);
    return new TimeSeriesBuilderBlock(_timeBuckets, seriesBuilderMap);
  }

  private void processLiteral(int numDocs, int[] timeValueIndexes) {
    if (_timeBuckets.getNumBuckets() < (numDocs / 2) && _timeBuckets.getNumBuckets() < THRESHOLD_FOR_LITERAL_OPT) {
      int[] frequencyCount = new int[_timeBuckets.getNumBuckets()];
      for (int docIndex = 0; docIndex < numDocs; docIndex++) {
        frequencyCount[timeValueIndexes[docIndex]]++;
      }
      for (int timeIndex = 0; timeIndex < frequencyCount.length; timeIndex++) {
        if (frequencyCount[timeIndex] > 0) {
          _emptyGroupBySeriesBuilder.addValueAtIndex(timeIndex, _literalValue, frequencyCount[timeIndex]);
        }
      }
      return;
    }
    for (int docIndex = 0; docIndex < numDocs; docIndex++) {
      _emptyGroupBySeriesBuilder.addValueAtIndex(timeValueIndexes[docIndex], _literalValue);
    }
  }

  private void processLongExpression(int numDocs, BlockValSet blockValSet, int[] timeValueIndexes) {
    long[] values = blockValSet.getLongValuesSV();
    for (int docIndex = 0; docIndex < numDocs; docIndex++) {
      _emptyGroupBySeriesBuilder.addValueAtIndex(timeValueIndexes[docIndex], (double) values[docIndex]);
    }
  }

  private void processDoubleExpression(int numDocs, BlockValSet blockValSet, int[] timeValueIndexes) {
    double[] values = blockValSet.getDoubleValuesSV();
    for (int docIndex = 0; docIndex < numDocs; docIndex++) {
      _emptyGroupBySeriesBuilder.addValueAtIndex(timeValueIndexes[docIndex], values[docIndex]);
    }
  }

  private void processIntExpression(int numDocs, BlockValSet blockValSet, int[] timeValueIndexes) {
    int[] values = blockValSet.getIntValuesSV();
    for (int docIndex = 0; docIndex < numDocs; docIndex++) {
      _emptyGroupBySeriesBuilder.addValueAtIndex(timeValueIndexes[docIndex], (double) values[docIndex]);
    }
  }

  private void processStringExpression(int numDocs, BlockValSet blockValSet, int[] timeValueIndexes) {
    String[] values = blockValSet.getStringValuesSV();
    for (int docIndex = 0; docIndex < numDocs; docIndex++) {
      _emptyGroupBySeriesBuilder.addValueAtIndex(timeValueIndexes[docIndex], values[docIndex]);
    }
  }
}
