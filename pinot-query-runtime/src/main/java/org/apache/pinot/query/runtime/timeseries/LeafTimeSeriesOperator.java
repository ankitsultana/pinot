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
package org.apache.pinot.query.runtime.timeseries;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.core.operator.blocks.InstanceResponseBlock;
import org.apache.pinot.core.operator.blocks.results.AggregationResultsBlock;
import org.apache.pinot.core.operator.blocks.results.GroupByResultsBlock;
import org.apache.pinot.core.operator.blocks.results.TimeSeriesResultsBlock;
import org.apache.pinot.core.query.executor.QueryExecutor;
import org.apache.pinot.core.query.logger.ServerQueryLogger;
import org.apache.pinot.core.query.request.ServerQueryRequest;
import org.apache.pinot.tsdb.spi.operator.BaseTimeSeriesOperator;
import org.apache.pinot.tsdb.spi.series.TimeSeries;
import org.apache.pinot.tsdb.spi.series.TimeSeriesBlock;


public class LeafTimeSeriesOperator extends BaseTimeSeriesOperator {
  private final TimeSeriesExecutionContext _context;
  private final ServerQueryRequest _request;
  private final QueryExecutor _queryExecutor;
  private final ExecutorService _executorService;
  private final ServerQueryLogger _queryLogger;

  public LeafTimeSeriesOperator(TimeSeriesExecutionContext context, ServerQueryRequest serverQueryRequest,
      QueryExecutor queryExecutor, ExecutorService executorService) {
    super(Collections.emptyList());
    _context = context;
    _request = serverQueryRequest;
    _queryExecutor = queryExecutor;
    _executorService = executorService;
    _queryLogger = ServerQueryLogger.getInstance();
  }

  @Override
  public TimeSeriesBlock getNextBlock() {
    Preconditions.checkNotNull(_queryExecutor, "Leaf time series operator has not been initialized");
    InstanceResponseBlock instanceResponseBlock = _queryExecutor.execute(_request, _executorService);
    assert !(instanceResponseBlock.getResultsBlock() instanceof TimeSeriesResultsBlock);
    _queryLogger.logQuery(_request, instanceResponseBlock, "TimeSeries");
    if (instanceResponseBlock.getResultsBlock() instanceof GroupByResultsBlock) {
      return handleGroupByResultsBlock((GroupByResultsBlock) instanceResponseBlock.getResultsBlock());
    } else if (instanceResponseBlock.getResultsBlock() instanceof AggregationResultsBlock) {
      return buildAggregationResultsBlock((AggregationResultsBlock) instanceResponseBlock.getResultsBlock());
    } else {
      throw new UnsupportedOperationException("Unknown results block");
    }
  }

  @Override
  public String getExplainName() {
    return "TIME_SERIES_LEAF_STAGE_OPERATOR";
  }

  private TimeSeriesBlock handleGroupByResultsBlock(GroupByResultsBlock groupByResultsBlock) {
    Iterator<Record> iter = groupByResultsBlock.getTable().iterator();
    Map<Long, List<TimeSeries>> timeSeriesMap = new HashMap<>(groupByResultsBlock.getNumRows());
    List<String> tagNames = new ArrayList<>();
    int numColumns = groupByResultsBlock.getDataSchema().getColumnNames().length - 1;
    for (int index = 0; index < numColumns; index++) {
      tagNames.add(groupByResultsBlock.getDataSchema().getColumnName(index));
    }
    while (iter.hasNext()) {
      Record record = iter.next();
      Object[] values = record.getValues();
      Object[] tagValues = new Object[values.length - 1];
      for (int index = 0; index + 1 < values.length; index++) {
        tagValues[index] = values[index] == null ? "null" : values[index].toString();
      }
      Double[] doubleValues = (Double[]) values[values.length - 1];
      long seriesHash = TimeSeries.hash(tagValues);
      List<TimeSeries> timeSeriesList = new ArrayList<>(1);
      timeSeriesList.add(new TimeSeries(Long.toString(seriesHash), null, _context.getInitialTimeBuckets(),
          doubleValues, tagNames, tagValues));
      timeSeriesMap.put(seriesHash, timeSeriesList);
    }
    return new TimeSeriesBlock(_context.getInitialTimeBuckets(), timeSeriesMap);
  }

  private TimeSeriesBlock buildAggregationResultsBlock(AggregationResultsBlock aggregationResultsBlock) {
    Map<Long, List<TimeSeries>> timeSeriesMap = new HashMap<>();
    Double[] doubleValues = (Double[]) aggregationResultsBlock.getResults().get(0);
    long seriesHash = TimeSeries.hash(new Object[0]);
    List<TimeSeries> timeSeriesList = new ArrayList<>(1);
    timeSeriesList.add(new TimeSeries(Long.toString(seriesHash), null, _context.getInitialTimeBuckets(),
        doubleValues, Collections.emptyList(), new Object[0]));
    timeSeriesMap.put(seriesHash, timeSeriesList);
    return new TimeSeriesBlock(_context.getInitialTimeBuckets(), timeSeriesMap);
  }
}
