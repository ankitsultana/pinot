/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.core.operator.timeseries;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.operator.BaseProjectOperator;
import org.apache.pinot.core.operator.ExecutionStatistics;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.blocks.results.TimeSeriesResultsBlock;
import org.apache.pinot.tsdb.spi.AggInfo;
import org.apache.pinot.tsdb.spi.TimeBuckets;
import org.apache.pinot.tsdb.spi.series.BaseSeriesBuilder;
import org.apache.pinot.tsdb.spi.series.Series;
import org.apache.pinot.tsdb.spi.series.SeriesBlock;
import org.apache.pinot.tsdb.spi.series.SeriesBuilderFactory;


/**
 * Segment level operator which converts data in relational model to a time-series model, by aggregating on a
 * configured aggregation expression.
 */
public class TimeSeriesAggregationOperator extends BaseOperator<TimeSeriesResultsBlock> {
  private static final String EXPLAIN_NAME = "TIME_SERIES_AGGREGATION";
  private final String _timeColumn;
  private final Long _timeOffset;
  private final AggInfo _aggInfo;
  private final ExpressionContext _valueExpression;
  private final List<String> _groupByExpressions;
  private final BaseProjectOperator<? extends ValueBlock> _projectOperator;
  private final TimeBuckets _timeBuckets;
  private final SeriesBuilderFactory _seriesBuilderFactory;

  public TimeSeriesAggregationOperator(
      String timeColumn,
      Long timeOffset,
      AggInfo aggInfo,
      ExpressionContext valueExpression,
      List<String> groupByExpressions,
      TimeBuckets timeBuckets,
      BaseProjectOperator<? extends ValueBlock> projectOperator,
      SeriesBuilderFactory seriesBuilderFactory) {
    _timeColumn = timeColumn;
    _timeOffset = timeOffset;
    _aggInfo = aggInfo;
    _valueExpression = valueExpression;
    _groupByExpressions = groupByExpressions;
    _projectOperator = projectOperator;
    _timeBuckets = timeBuckets;
    _seriesBuilderFactory = seriesBuilderFactory;
  }

  @Override
  protected TimeSeriesResultsBlock getNextBlock() {
    ValueBlock transformBlock = _projectOperator.nextBlock();
    BlockValSet blockValSet = transformBlock.getBlockValueSet(_timeColumn);
    long[] timeValues = blockValSet.getLongValuesSV();
    if (_timeOffset != null && _timeOffset != 0L) {
      timeValues = applyTimeshift(_timeOffset, timeValues);
    }
    int[] timeValueIndexes = getTimeValueIndex(timeValues, inferTimeUnit(timeValues));
    Object[][] tagValues = new Object[_groupByExpressions.size()][];
    Map<Long, BaseSeriesBuilder> seriesBuilderMap = new HashMap<>(1024);
    for (int i = 0; i < _groupByExpressions.size(); i++) {
      blockValSet = transformBlock.getBlockValueSet(_groupByExpressions.get(i));
      switch (blockValSet.getValueType()) {
        case STRING:
          tagValues[i] = blockValSet.getStringValuesSV();
          break;
        case LONG:
          tagValues[i] = ArrayUtils.toObject(blockValSet.getLongValuesSV());
          break;
        default:
          throw new NotImplementedException("Can't handle types other than string and long");
      }
    }
    BlockValSet valueExpressionBlockValSet = transformBlock.getBlockValueSet(_valueExpression);
    switch (valueExpressionBlockValSet.getValueType()) {
      case LONG:
        processLongExpression(valueExpressionBlockValSet, seriesBuilderMap, timeValueIndexes, tagValues);
        break;
      case INT:
        processIntExpression(valueExpressionBlockValSet, seriesBuilderMap, timeValueIndexes, tagValues);
        break;
      case DOUBLE:
        processDoubleExpression(valueExpressionBlockValSet, seriesBuilderMap, timeValueIndexes, tagValues);
        break;
      case STRING:
        processStringExpression(valueExpressionBlockValSet, seriesBuilderMap, timeValueIndexes, tagValues);
        break;
      default:
        throw new IllegalStateException("");
    }
    Map<Long, List<Series>> seriesMap = new HashMap<>();
    for (var entry : seriesBuilderMap.entrySet()) {
      List<Series> seriesList = new ArrayList<>();
      seriesList.add(entry.getValue().build());
      seriesMap.put(entry.getKey(), seriesList);
    }
    return new TimeSeriesResultsBlock(new SeriesBlock(_timeBuckets, seriesMap));
  }

  @Override
  @SuppressWarnings("rawtypes")
  public List<? extends Operator> getChildOperators() {
    return ImmutableList.of(_projectOperator);
  }

  @Nullable
  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  @Override
  public ExecutionStatistics getExecutionStatistics() {
    return new ExecutionStatistics(0, 0, 0, 0);
  }

  private int[] getTimeValueIndex(long[] actualTimeValues, TimeUnit timeUnit) {
    if (timeUnit == TimeUnit.MILLISECONDS) {
      return getTimeValueIndexMillis(actualTimeValues);
    }
    int[] timeIndexes = new int[actualTimeValues.length];
    for (int index = 0; index < actualTimeValues.length; index++) {
      timeIndexes[index] = (int) ((actualTimeValues[index] - _timeBuckets.getStartTime()) / _timeBuckets.getBucketSize().getSeconds());
    }
    return timeIndexes;
  }

  private int[] getTimeValueIndexMillis(long[] actualTimeValues) {
    int[] timeIndexes = new int[actualTimeValues.length];
    for (int index = 0; index < actualTimeValues.length; index++) {
      timeIndexes[index] = (int) ((actualTimeValues[index] - _timeBuckets.getStartTime() * 1000L)
          / _timeBuckets.getRangeSeconds());
    }
    return timeIndexes;
  }

  public void processLongExpression(BlockValSet blockValSet, Map<Long, BaseSeriesBuilder> seriesBuilderMap,
      int[] timeValueIndexes, Object[][] tagValues) {
    long[] valueColumnValues = blockValSet.getLongValuesSV();
    for (int docIdIndex = 0; docIdIndex < timeValueIndexes.length; docIdIndex++) {
      Object[] tagValuesForDoc = new Object[_groupByExpressions.size()];
      for (int tagIndex = 0; tagIndex < tagValues.length; tagIndex++) {
        tagValuesForDoc[tagIndex] = tagValues[tagIndex][docIdIndex];
      }
      long hash = Series.hash(tagValuesForDoc);
      seriesBuilderMap.computeIfAbsent(hash,
              k -> _seriesBuilderFactory.newSeriesBuilder(_aggInfo, Long.toString(hash), _timeBuckets,
                  _groupByExpressions,
                  tagValuesForDoc))
          .addValueAtIndex(timeValueIndexes[docIdIndex], (double) valueColumnValues[docIdIndex]);
    }
  }

  public void processIntExpression(BlockValSet blockValSet, Map<Long, BaseSeriesBuilder> seriesBuilderMap,
      int[] timeValueIndexes, Object[][] tagValues) {
    int[] valueColumnValues = blockValSet.getIntValuesSV();
    for (int docIdIndex = 0; docIdIndex < timeValueIndexes.length; docIdIndex++) {
      Object[] tagValuesForDoc = new Object[_groupByExpressions.size()];
      for (int tagIndex = 0; tagIndex < tagValues.length; tagIndex++) {
        tagValuesForDoc[tagIndex] = tagValues[tagIndex][docIdIndex];
      }
      long hash = Series.hash(tagValuesForDoc);
      seriesBuilderMap.computeIfAbsent(hash,
              k -> _seriesBuilderFactory.newSeriesBuilder(_aggInfo, Long.toString(hash), _timeBuckets,
                  _groupByExpressions,
                  tagValuesForDoc))
          .addValueAtIndex(timeValueIndexes[docIdIndex], (double) valueColumnValues[docIdIndex]);
    }
  }

  public void processDoubleExpression(BlockValSet blockValSet, Map<Long, BaseSeriesBuilder> seriesBuilderMap,
      int[] timeValueIndexes, Object[][] tagValues) {
    double[] valueColumnValues = blockValSet.getDoubleValuesSV();
    for (int docIdIndex = 0; docIdIndex < timeValueIndexes.length; docIdIndex++) {
      Object[] tagValuesForDoc = new Object[_groupByExpressions.size()];
      for (int tagIndex = 0; tagIndex < tagValues.length; tagIndex++) {
        tagValuesForDoc[tagIndex] = tagValues[tagIndex][docIdIndex];
      }
      long hash = Series.hash(tagValuesForDoc);
      seriesBuilderMap.computeIfAbsent(hash,
              k -> _seriesBuilderFactory.newSeriesBuilder(_aggInfo, Long.toString(hash), _timeBuckets,
                  _groupByExpressions,
                  tagValuesForDoc))
          .addValueAtIndex(timeValueIndexes[docIdIndex], valueColumnValues[docIdIndex]);
    }
  }

  public void processStringExpression(BlockValSet blockValSet, Map<Long, BaseSeriesBuilder> seriesBuilderMap,
      int[] timeValueIndexes, Object[][] tagValues) {
    String[] valueColumnValues = blockValSet.getStringValuesSV();
    for (int docIdIndex = 0; docIdIndex < timeValueIndexes.length; docIdIndex++) {
      Object[] tagValuesForDoc = new Object[_groupByExpressions.size()];
      for (int tagIndex = 0; tagIndex < tagValues.length; tagIndex++) {
        tagValuesForDoc[tagIndex] = tagValues[tagIndex][docIdIndex];
      }
      long hash = Series.hash(tagValuesForDoc);
      seriesBuilderMap.computeIfAbsent(hash,
              k -> _seriesBuilderFactory.newSeriesBuilder(_aggInfo, Long.toString(hash), _timeBuckets,
                  _groupByExpressions, tagValuesForDoc))
          .addValueAtIndex(timeValueIndexes[docIdIndex], valueColumnValues[docIdIndex]);
    }
  }

  public static long[] applyTimeshift(long timeshift, long[] timeValues) {
    if (timeshift == 0) {
      return timeValues;
    }
    long[] shiftedTimeValues = new long[timeValues.length];
    for (int index = 0; index < timeValues.length; index++) {
      shiftedTimeValues[index] = timeValues[index] + timeshift;
    }
    return shiftedTimeValues;
  }

  @Nullable
  public static TimeUnit inferTimeUnit(long[] timeValues) {
    // TODO: Improve this logic.
    if (timeValues == null || timeValues.length == 0) {
      return null;
    }
    if (isTimestampMillis(timeValues[0])) {
      return TimeUnit.MILLISECONDS;
    }
    return TimeUnit.SECONDS;
  }

  public static boolean isTimestampMillis(long timestamp) {
    // 946684800000 is 01 Jan 2000 00:00:00 GMT in Epoch Millis.
    // 946684800000 in epoch seconds is ~30k years in the future.
    // As long as the timestamp is after year 2000 and less than year ~30_000, this function will return the correct result
    return timestamp >= 946684800000L;
  }
}