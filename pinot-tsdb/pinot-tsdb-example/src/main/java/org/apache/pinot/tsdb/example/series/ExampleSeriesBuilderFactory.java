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
package org.apache.pinot.tsdb.example.series;

import java.util.List;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.tsdb.example.Aggregations;
import org.apache.pinot.tsdb.spi.AggInfo;
import org.apache.pinot.tsdb.spi.TimeBuckets;
import org.apache.pinot.tsdb.spi.series.BaseSeriesBuilder;
import org.apache.pinot.tsdb.spi.series.SeriesBuilderFactory;
import org.apache.pinot.tsdb.spi.series.builders.MaxSeriesBuilder;
import org.apache.pinot.tsdb.spi.series.builders.MinSeriesBuilder;
import org.apache.pinot.tsdb.spi.series.builders.SummingSeriesBuilder;


public class ExampleSeriesBuilderFactory extends SeriesBuilderFactory {
  @Override
  public BaseSeriesBuilder newSeriesBuilder(AggInfo aggInfo, String id, TimeBuckets timeBuckets, List<String> tagNames,
      Object[] tagValues) {
    Aggregations.AggType aggType = Aggregations.AggType.valueOf(aggInfo.getAggFunction().toUpperCase());
    switch (aggType) {
      case SUM:
        return new SummingSeriesBuilder(id, timeBuckets, tagNames, tagValues);
      case MIN:
        return new MinSeriesBuilder(id, timeBuckets, tagNames, tagValues);
      case MAX:
        return new MaxSeriesBuilder(id, timeBuckets, tagNames, tagValues);
      default:
        throw new UnsupportedOperationException("Unsupported aggregation type: " + aggType);
    }
  }

  @Override
  public void init(PinotConfiguration pinotConfiguration) {
  }
}
