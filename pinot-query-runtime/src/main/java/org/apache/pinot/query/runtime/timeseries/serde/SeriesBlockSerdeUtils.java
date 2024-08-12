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

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.response.PrometheusResponse;
import org.apache.pinot.tsdb.spi.series.Series;
import org.apache.pinot.tsdb.spi.series.SeriesBlock;


public class SeriesBlockSerdeUtils {
  public static final SeriesBlockSerdeUtils INSTANCE = new SeriesBlockSerdeUtils();

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private SeriesBlockSerdeUtils() {
  }

  public static PrometheusResponse convertToPrometheusResponse(SeriesBlockSerialized seriesBlockSerialized) {
    Long[] timeValues = seriesBlockSerialized.getTimeBuckets();
    List<PrometheusResponse.Value> result = new ArrayList<>();
    for (var entry : seriesBlockSerialized.getSeriesMap().entrySet()) {
      SeriesBlockEntry seriesBlockEntry = entry.getValue();
      Map<String, String> metric = seriesBlockEntry.getTagsMap();
      Object[][] values = seriesBlockEntry.getValues();
      for (Object[] singleValue : values) {
        Object[][] promValues = new Object[timeValues.length][];
        for (int index = 0; index < timeValues.length; index++) {
          promValues[index] = new Object[2];
          promValues[index][0] = timeValues[index];
          promValues[index][1] = singleValue[index];
        }
        result.add(new PrometheusResponse.Value(metric, promValues));
      }
    }
    PrometheusResponse.Data data = new PrometheusResponse.Data("matrix", result);
    return new PrometheusResponse("success", data);
  }

  public static String serialize(SeriesBlock seriesBlock)
      throws Exception {
    Map<Long, SeriesBlockEntry> entryMap = new HashMap<>();
    for (var entry : seriesBlock.getSeriesMap().entrySet()) {
      entryMap.put(entry.getKey(), convertToSeriesBlockEntry(entry.getValue()));
    }
    Long[] timeBucketValues = seriesBlock.getTimeBuckets().getTimeBuckets();
    SeriesBlockSerialized serialized = new SeriesBlockSerialized(timeBucketValues, entryMap);
    return OBJECT_MAPPER.writeValueAsString(serialized);
  }

  public static SeriesBlockEntry convertToSeriesBlockEntry(List<Series> seriesList) {
    Double[][] values = new Double[seriesList.size()][];
    for (int i = 0; i < values.length; i++) {
      values[i] = seriesList.get(i).getValues();
    }
    Series anySeries = seriesList.get(0);
    return new SeriesBlockEntry(anySeries.getTagsSerialized(), anySeries.getTagKeyValuesAsMap(), values);
  }
}
