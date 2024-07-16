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
package org.apache.pinot.query.runtime.timeseries.serde;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;


/**
 * <pre>
 *   {
 *     "timeBuckets": [t0, t1, ... tn-1],
 *     "seriesMap": {
 *       "seriesHash1": {
 *         "seriesName": "",
 *         "values": [
 *           [v00, v01, ... v0n-1],
 *           [v10, v11, ... v1n-1],
 *           ...
 *         ]
 *       }
 *     }
 *   }
 * </pre>
 */
public class SeriesBlockSerialized {
  private final Long[] _timeBuckets;
  private final Map<Long, SeriesBlockEntry> _seriesMap;

  @JsonCreator
  public SeriesBlockSerialized(@JsonProperty("timeBuckets") Long[] timeBuckets,
      @JsonProperty("seriesMap") Map<Long, SeriesBlockEntry> seriesMap) {
    _timeBuckets = timeBuckets;
    _seriesMap = seriesMap;
  }

  public Long[] getTimeBuckets() {
    return _timeBuckets;
  }

  public Map<Long, SeriesBlockEntry> getSeriesMap() {
    return _seriesMap;
  }
}
