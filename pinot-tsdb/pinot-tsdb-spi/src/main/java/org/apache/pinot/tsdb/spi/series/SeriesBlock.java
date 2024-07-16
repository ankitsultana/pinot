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
