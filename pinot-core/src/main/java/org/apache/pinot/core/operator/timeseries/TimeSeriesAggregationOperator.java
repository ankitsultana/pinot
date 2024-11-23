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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.operator.BaseProjectOperator;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.ExecutionStatistics;
import org.apache.pinot.core.operator.blocks.TimeSeriesBuilderBlock;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.blocks.results.TimeSeriesResultsBlock;
import org.apache.pinot.core.operator.timeseries.executor.TimeSeriesEmptyGroupByExecutor;
import org.apache.pinot.core.operator.timeseries.executor.TimeSeriesGroupByExecutor;
import org.apache.pinot.core.operator.timeseries.executor.TimeSeriesNonEmptyGroupByExecutor;
import org.apache.pinot.core.plan.maker.InstancePlanMakerImplV2;
import org.apache.pinot.core.query.aggregation.groupby.DictionaryBasedGroupKeyGenerator;
import org.apache.pinot.core.query.aggregation.groupby.GroupKeyGenerator;
import org.apache.pinot.core.query.aggregation.groupby.NoDictionaryMultiColumnGroupKeyGenerator;
import org.apache.pinot.core.query.aggregation.groupby.NoDictionarySingleColumnGroupKeyGenerator;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.tsdb.spi.AggInfo;
import org.apache.pinot.tsdb.spi.TimeBuckets;
import org.apache.pinot.tsdb.spi.series.BaseTimeSeriesBuilder;
import org.apache.pinot.tsdb.spi.series.TimeSeries;
import org.apache.pinot.tsdb.spi.series.TimeSeriesBuilderFactory;


/**
 * Segment level operator which converts data in relational model to a time-series model, by aggregating on a
 * configured aggregation expression.
 */
public class TimeSeriesAggregationOperator extends BaseOperator<TimeSeriesResultsBlock> {
  private static final String EXPLAIN_NAME = "TIME_SERIES_AGGREGATION";
  private final String _timeColumn;
  private final TimeUnit _storedTimeUnit;
  private final long _timeOffset;
  private final AggInfo _aggInfo;
  private final ExpressionContext _valueExpression;
  private final List<String> _groupByExpressions;
  private final BaseProjectOperator<? extends ValueBlock> _projectOperator;
  private final TimeBuckets _timeBuckets;
  private final TimeSeriesBuilderFactory _seriesBuilderFactory;
  private final int _maxSeriesLimit;
  private final long _maxDataPointsLimit;
  private final long _numTotalDocs;
  private final BaseTimeSeriesBuilder _emptyGroupSetSeriesBuilder;
  private final TimeSeriesGroupByExecutor _groupByExecutor;
  private long _numDocsScanned = 0;

  public TimeSeriesAggregationOperator(
      String timeColumn,
      TimeUnit timeUnit,
      @Nullable Long timeOffsetSeconds,
      AggInfo aggInfo,
      ExpressionContext valueExpression,
      List<String> groupByExpressions,
      TimeBuckets timeBuckets,
      BaseProjectOperator<? extends ValueBlock> projectOperator,
      TimeSeriesBuilderFactory seriesBuilderFactory,
      SegmentMetadata segmentMetadata) {
    _timeColumn = timeColumn;
    _storedTimeUnit = timeUnit;
    _timeOffset = timeOffsetSeconds == null ? 0L : timeUnit.convert(Duration.ofSeconds(timeOffsetSeconds));
    _aggInfo = aggInfo;
    _valueExpression = valueExpression;
    _groupByExpressions = groupByExpressions;
    _projectOperator = projectOperator;
    _timeBuckets = timeBuckets;
    _seriesBuilderFactory = seriesBuilderFactory;
    _maxSeriesLimit = _seriesBuilderFactory.getMaxUniqueSeriesPerServerLimit();
    _maxDataPointsLimit = _seriesBuilderFactory.getMaxDataPointsPerServerLimit();
    _numTotalDocs = segmentMetadata.getTotalDocs();
    if (_groupByExpressions.isEmpty()) {
      Object[] emptyTagValues = new Object[0];
      long hash = TimeSeries.hash(emptyTagValues);
      _emptyGroupSetSeriesBuilder = _seriesBuilderFactory.newTimeSeriesBuilder(_aggInfo, Long.toString(hash),
          _timeBuckets, Collections.emptyList(), emptyTagValues);
    } else {
      _emptyGroupSetSeriesBuilder = null;
    }
    // Initialize group by executor
    // TODO(timeseries): What's the right way to handle null handling for both value and dimensions?
    ExpressionContext[] groupByExpressionCompiled =
        groupByExpressions.stream().map(RequestContextUtils::getExpression).toArray(ExpressionContext[]::new);
    boolean hasNoDictionaryGroupByExpression = false;
    for (ExpressionContext groupByExpression : groupByExpressionCompiled) {
      ColumnContext columnContext = _projectOperator.getResultColumnContext(groupByExpression);
      Preconditions.checkState(columnContext.isSingleValue(), "Multi-value columns not supported yet");
      hasNoDictionaryGroupByExpression |= columnContext.getDictionary() == null;
    }
    if (_groupByExpressions.isEmpty()) {
      _groupByExecutor = new TimeSeriesEmptyGroupByExecutor(_emptyGroupSetSeriesBuilder, _timeBuckets,
          _valueExpression);
    } else if (hasNoDictionaryGroupByExpression) {
      Supplier<GroupKeyGenerator> groupKeyGeneratorSupplier;
      if (_groupByExpressions.size() == 1) {
        groupKeyGeneratorSupplier = () -> {
          return new NoDictionarySingleColumnGroupKeyGenerator(_projectOperator,
              groupByExpressionCompiled[0], _maxSeriesLimit, false, null);
        };
      } else {
        groupKeyGeneratorSupplier = () -> {
          return new NoDictionaryMultiColumnGroupKeyGenerator(_projectOperator,
              groupByExpressionCompiled, _maxSeriesLimit, false,
              null);
        };
      }
      _groupByExecutor = new TimeSeriesNonEmptyGroupByExecutor(_aggInfo, _timeBuckets, _valueExpression,
            _groupByExpressions, _seriesBuilderFactory, groupKeyGeneratorSupplier);
    } else {
      Supplier<GroupKeyGenerator> groupKeyGeneratorSupplier = () -> {
        return new DictionaryBasedGroupKeyGenerator(_projectOperator, groupByExpressionCompiled, _maxSeriesLimit,
            InstancePlanMakerImplV2.DEFAULT_MAX_INITIAL_RESULT_HOLDER_CAPACITY, null);
      };
      _groupByExecutor = new TimeSeriesNonEmptyGroupByExecutor(_aggInfo, _timeBuckets, _valueExpression,
            groupByExpressions, _seriesBuilderFactory, groupKeyGeneratorSupplier);
    }
  }

  @Override
  public TimeSeriesResultsBlock getNextBlock() {
    ValueBlock valueBlock;
    int[] timeValueIndexes = null;
    while ((valueBlock = _projectOperator.nextBlock()) != null) {
      int numDocs = valueBlock.getNumDocs();
      _numDocsScanned += numDocs;
      BlockValSet blockValSet = valueBlock.getBlockValueSet(_timeColumn);
      long[] timeValues = blockValSet.getLongValuesSV();
      applyTimeOffset(timeValues, numDocs);
      if (timeValueIndexes == null || timeValueIndexes.length < numDocs) {
        timeValueIndexes = new int[numDocs];
      }
      populateTimeValueIndex(timeValues, numDocs, timeValueIndexes);
      _groupByExecutor.process(numDocs, timeValueIndexes, valueBlock);
      Map<Long, BaseTimeSeriesBuilder> seriesBuilderMap = _groupByExecutor.getTimeSeriesBuilderBlock()
          .getSeriesBuilderMap();
      Preconditions.checkState(seriesBuilderMap.size() * (long) _timeBuckets.getNumBuckets() <= _maxDataPointsLimit,
          "Exceeded max data point limit per server. Limit: %s. Data points in current segment so far: %s",
          _maxDataPointsLimit, seriesBuilderMap.size() * _timeBuckets.getNumBuckets());
      Preconditions.checkState(seriesBuilderMap.size() <= _maxSeriesLimit,
          "Exceeded max unique series limit per server. Limit: %s. Series in current segment so far: %s",
          _maxSeriesLimit, seriesBuilderMap.size());
    }
    TimeSeriesBuilderBlock seriesBuilderBlock = _groupByExecutor.getTimeSeriesBuilderBlock();
    return new TimeSeriesResultsBlock(seriesBuilderBlock);
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
    long numEntriesScannedInFilter = _projectOperator.getExecutionStatistics().getNumEntriesScannedInFilter();
    long numEntriesScannedPostFilter = _numDocsScanned * _projectOperator.getNumColumnsProjected();
    return new ExecutionStatistics(_numDocsScanned, numEntriesScannedInFilter, numEntriesScannedPostFilter,
        _numTotalDocs);
  }

  @VisibleForTesting
  protected void populateTimeValueIndex(long[] actualTimeValues, int numDocs, int[] timeIndexesBuffer) {
    if (_storedTimeUnit == TimeUnit.MILLISECONDS) {
      populateTimeValueIndexMillis(actualTimeValues, numDocs, timeIndexesBuffer);
      return;
    }
    final long reference = _timeBuckets.getTimeRangeStartExclusive();
    final long divisor = _timeBuckets.getBucketSize().getSeconds();
    for (int index = 0; index < numDocs; index++) {
      timeIndexesBuffer[index] = (int) ((actualTimeValues[index] - reference - 1) / divisor);
    }
  }

  private void populateTimeValueIndexMillis(long[] actualTimeValues, int numDocs, int[] timeIndexes) {
    final long reference = _timeBuckets.getTimeRangeStartExclusive() * 1000L;
    final long divisor = _timeBuckets.getBucketSize().toMillis();
    for (int index = 0; index < numDocs; index++) {
      timeIndexes[index] = (int) ((actualTimeValues[index] - reference - 1) / divisor);
    }
  }

  private void applyTimeOffset(long[] timeValues, int numDocs) {
    if (_timeOffset == 0L) {
      return;
    }
    for (int index = 0; index < numDocs; index++) {
      timeValues[index] = timeValues[index] + _timeOffset;
    }
  }
}
