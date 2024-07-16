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
package org.apache.pinot.core.operator.combine.merger;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.core.operator.blocks.results.TimeSeriesResultsBlock;
import org.apache.pinot.tsdb.spi.AggInfo;
import org.apache.pinot.tsdb.spi.series.BaseSeriesBuilder;
import org.apache.pinot.tsdb.spi.series.Series;
import org.apache.pinot.tsdb.spi.series.SeriesBlock;
import org.apache.pinot.tsdb.spi.series.SeriesBuilderFactory;


public class TimeSeriesAggResultsBlockMerger implements ResultsBlockMerger<TimeSeriesResultsBlock> {
  private final SeriesBuilderFactory _seriesBuilderFactory;
  private final AggInfo _aggInfo;

  public TimeSeriesAggResultsBlockMerger(SeriesBuilderFactory seriesBuilderFactory, AggInfo aggInfo) {
    _seriesBuilderFactory = seriesBuilderFactory;
    _aggInfo = aggInfo;
  }

  @Override
  public void mergeResultsBlocks(TimeSeriesResultsBlock mergedBlock, TimeSeriesResultsBlock blockToMerge) {
    SeriesBlock currentSeriesBlock = mergedBlock.getSeriesBlock();
    SeriesBlock seriesBlockToMerge = blockToMerge.getSeriesBlock();
    for (var entry : seriesBlockToMerge.getSeriesMap().entrySet()) {
      long seriesHash = entry.getKey();
      List<Series> currentSeriesList = currentSeriesBlock.getSeriesMap().get(seriesHash);
      Series currentSeries = null;
      if (currentSeriesList != null && !currentSeriesList.isEmpty()) {
        currentSeries = currentSeriesList.get(0);
      }
      Series newSeriesToMerge = entry.getValue().get(0);
      if (currentSeries == null) {
        List<Series> newSeriesList = new ArrayList<>();
        newSeriesList.add(newSeriesToMerge);
        currentSeriesBlock.getSeriesMap().put(seriesHash, newSeriesList);
      } else {
        BaseSeriesBuilder mergedSeriesBuilder = _seriesBuilderFactory.newSeriesBuilder(
            _aggInfo, currentSeries.getId(), currentSeries.getTimeBuckets(), currentSeries.getTagNames(),
            currentSeries.getTagValues());
        mergedSeriesBuilder.mergeAlignedSeries(newSeriesToMerge);
        currentSeriesBlock.getSeriesMap().put(seriesHash, ImmutableList.of(mergedSeriesBuilder.build()));
      }
    }
  }
}
