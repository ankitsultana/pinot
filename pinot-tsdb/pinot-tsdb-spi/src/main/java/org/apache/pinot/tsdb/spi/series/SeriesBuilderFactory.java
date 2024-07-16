package org.apache.pinot.tsdb.spi.series;

import java.util.List;
import org.apache.pinot.tsdb.spi.AggType;
import org.apache.pinot.tsdb.spi.TimeBuckets;


public abstract class SeriesBuilderFactory {
  public abstract BaseSeriesBuilder newSeriesBuilder(
      AggType aggType,
      String id,
      TimeBuckets timeBuckets,
      List<String> tagNames,
      Object[] tagValues);

  public abstract BaseSeriesBuilder newSeriesBuilder(AggType aggType, Series series);

  public abstract BaseSeriesBuilder newInstantSeriesBuilder(String id, List<String> tagNames, List<String> tagValues);
}
