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
