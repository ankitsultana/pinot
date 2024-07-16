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


public class SeriesBlockEntry {
  private final String _seriesName;
  private final Map<String, String> _tagsMap;
  private final Double[][] _values;

  @JsonCreator
  public SeriesBlockEntry(@JsonProperty("seriesName") String seriesName,
      @JsonProperty("tagsMap") Map<String, String> tagsMap,
      @JsonProperty("values") Double[][] values) {
    _seriesName = seriesName;
    _tagsMap = tagsMap;
    _values = values;
  }

  public String getSeriesName() {
    return _seriesName;
  }

  public Map<String, String> getTagsMap() {
    return _tagsMap;
  }

  public Double[][] getValues() {
    return _values;
  }
}
