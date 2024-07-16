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
package org.apache.pinot.tsdb.spi.series;

import java.util.List;
import java.util.Map;
import org.apache.pinot.tsdb.spi.TimeBuckets;


public class SeriesBlock {
  // time buckets represented in this series block
  private final TimeBuckets _timeBuckets;
  // series hash to series data
  private final Map<Long, List<Series>> _seriesMap;

  public SeriesBlock(TimeBuckets timeBuckets, Map<Long, List<Series>> seriesMap) {
    _timeBuckets = timeBuckets;
    _seriesMap = seriesMap;
  }

  public TimeBuckets getTimeBuckets() {
    return _timeBuckets;
  }

  public Map<Long, List<Series>> getSeriesMap() {
    return _seriesMap;
  }
}
