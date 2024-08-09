package org.apache.pinot.tsdb.spi.series;

import java.util.List;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.tsdb.spi.AggInfo;
import org.apache.pinot.tsdb.spi.TimeBuckets;


public abstract class SeriesBuilderFactory {
  public abstract BaseSeriesBuilder newSeriesBuilder(
      AggInfo aggInfo,
      String id,
      TimeBuckets timeBuckets,
      List<String> tagNames,
      Object[] tagValues);

  public abstract void init(PinotConfiguration pinotConfiguration);
}
