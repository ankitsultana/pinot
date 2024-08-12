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
