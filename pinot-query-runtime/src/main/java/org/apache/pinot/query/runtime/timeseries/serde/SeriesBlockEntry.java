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
