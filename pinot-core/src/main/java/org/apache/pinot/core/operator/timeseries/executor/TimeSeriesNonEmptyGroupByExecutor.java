package org.apache.pinot.core.operator.timeseries.executor;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.operator.blocks.TimeSeriesBuilderBlock;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.query.aggregation.groupby.GroupKeyGenerator;
import org.apache.pinot.tsdb.spi.AggInfo;
import org.apache.pinot.tsdb.spi.TimeBuckets;
import org.apache.pinot.tsdb.spi.series.BaseTimeSeriesBuilder;
import org.apache.pinot.tsdb.spi.series.TimeSeries;
import org.apache.pinot.tsdb.spi.series.TimeSeriesBuilderFactory;


public class TimeSeriesNonEmptyGroupByExecutor implements TimeSeriesGroupByExecutor {
  private final TimeBuckets _timeBuckets;
  private final AggInfo _aggInfo;
  private final Supplier<GroupKeyGenerator> _groupKeyGeneratorSupplier;
  private final ExpressionContext _valueExpression;
  private final List<String> _tagNames;
  private final Map<Long, BaseTimeSeriesBuilder> _seriesBuilderMap;
  private final TimeSeriesBuilderFactory _factory;
  private BaseTimeSeriesBuilder[] _seriesBuilderByGroupIndex;
  private int[] _groupKeysBuffer;

  public TimeSeriesNonEmptyGroupByExecutor(AggInfo aggInfo, TimeBuckets timeBuckets, ExpressionContext valueExpression,
      List<String> tagNames, TimeSeriesBuilderFactory factory, Supplier<GroupKeyGenerator> groupKeyGeneratorSupplier) {
    _aggInfo = aggInfo;
    _timeBuckets = timeBuckets;
    _valueExpression = valueExpression;
    _seriesBuilderMap = new HashMap<>(1000);
    _tagNames = tagNames;
    _factory = factory;
    _groupKeyGeneratorSupplier = groupKeyGeneratorSupplier;
  }

  @Override
  public void process(int numDocs, int[] timeValueIndexes, ValueBlock valueBlock) {
    if (_groupKeysBuffer == null || _groupKeysBuffer.length < numDocs) {
      _groupKeysBuffer = new int[numDocs];
    }
    GroupKeyGenerator groupKeyGenerator = _groupKeyGeneratorSupplier.get();
    groupKeyGenerator.generateKeysForBlock(valueBlock, _groupKeysBuffer);
    int numGroups = groupKeyGenerator.getNumKeys();
    if (_seriesBuilderByGroupIndex == null || _seriesBuilderByGroupIndex.length < numGroups) {
      _seriesBuilderByGroupIndex = new BaseTimeSeriesBuilder[numGroups];
    }
    GroupKeyGenerator.GroupKey groupKey;
    Iterator<GroupKeyGenerator.GroupKey> iter = groupKeyGenerator.getGroupKeys();
    while (iter.hasNext()) {
      groupKey = iter.next();
      long seriesHash = TimeSeries.hash(groupKey._keys);
      BaseTimeSeriesBuilder seriesBuilder = _seriesBuilderMap.get(seriesHash);
      if (seriesBuilder == null) {
        final Object[] keyClone = new Object[groupKey._keys.length];
        System.arraycopy(groupKey._keys, 0, keyClone, 0, keyClone.length);
        seriesBuilder = _factory.newTimeSeriesBuilder(_aggInfo, Long.toString(seriesHash), _timeBuckets, _tagNames,
            keyClone);
        _seriesBuilderMap.put(seriesHash, seriesBuilder);
      }
      _seriesBuilderByGroupIndex[groupKey._groupId] = seriesBuilder;
    }
    if (_valueExpression.getType() == ExpressionContext.Type.LITERAL) {
      processLiteralValue(numDocs, timeValueIndexes);
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
    return new TimeSeriesBuilderBlock(_timeBuckets, _seriesBuilderMap);
  }

  private void processLiteralValue(int numDocs, int[] timeValueIndexes) {
    Double value = _valueExpression.getLiteral().getDoubleValue();
    for (int docIndex = 0; docIndex < numDocs; docIndex++) {
      _seriesBuilderByGroupIndex[_groupKeysBuffer[docIndex]].addValueAtIndex(timeValueIndexes[docIndex], value);
    }
  }

  private void processLongExpression(int numDocs, BlockValSet blockValSet, int[] timeValueIndexes) {
    long[] values = blockValSet.getLongValuesSV();
    for (int docIndex = 0; docIndex < numDocs; docIndex++) {
      _seriesBuilderByGroupIndex[_groupKeysBuffer[docIndex]].addValueAtIndex(
          timeValueIndexes[docIndex], (double) values[docIndex]);
    }
  }

  private void processDoubleExpression(int numDocs, BlockValSet blockValSet, int[] timeValueIndexes) {
    double[] values = blockValSet.getDoubleValuesSV();
    for (int docIndex = 0; docIndex < numDocs; docIndex++) {
      _seriesBuilderByGroupIndex[_groupKeysBuffer[docIndex]].addValueAtIndex(
          timeValueIndexes[docIndex], (double) values[docIndex]);
    }
  }

  private void processIntExpression(int numDocs, BlockValSet blockValSet, int[] timeValueIndexes) {
    int[] values = blockValSet.getIntValuesSV();
    for (int docIndex = 0; docIndex < numDocs; docIndex++) {
      _seriesBuilderByGroupIndex[_groupKeysBuffer[docIndex]].addValueAtIndex(
          timeValueIndexes[docIndex], (double) values[docIndex]);
    }
  }

  private void processStringExpression(int numDocs, BlockValSet blockValSet, int[] timeValueIndexes) {
    String[] values = blockValSet.getStringValuesSV();
    for (int docIndex = 0; docIndex < numDocs; docIndex++) {
      _seriesBuilderByGroupIndex[_groupKeysBuffer[docIndex]].addValueAtIndex(
          timeValueIndexes[docIndex], values[docIndex]);
    }
  }
}
