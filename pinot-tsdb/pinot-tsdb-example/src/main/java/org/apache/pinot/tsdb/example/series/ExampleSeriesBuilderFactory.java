package org.apache.pinot.tsdb.example.series;

import java.util.List;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.tsdb.example.Aggregations;
import org.apache.pinot.tsdb.spi.AggInfo;
import org.apache.pinot.tsdb.spi.TimeBuckets;
import org.apache.pinot.tsdb.spi.series.BaseSeriesBuilder;
import org.apache.pinot.tsdb.spi.series.SeriesBuilderFactory;
import org.apache.pinot.tsdb.spi.series.builders.MaxSeriesBuilder;
import org.apache.pinot.tsdb.spi.series.builders.MinSeriesBuilder;
import org.apache.pinot.tsdb.spi.series.builders.SummingSeriesBuilder;


public class ExampleSeriesBuilderFactory extends SeriesBuilderFactory {
  @Override
  public BaseSeriesBuilder newSeriesBuilder(AggInfo aggInfo, String id, TimeBuckets timeBuckets, List<String> tagNames,
      Object[] tagValues) {
    Aggregations.AggType aggType = Aggregations.AggType.valueOf(aggInfo.getAggFunction().toUpperCase());
    switch (aggType) {
      case SUM:
        return new SummingSeriesBuilder(id, timeBuckets, tagNames, tagValues);
      case MIN:
        return new MinSeriesBuilder(id, timeBuckets, tagNames, tagValues);
      case MAX:
        return new MaxSeriesBuilder(id, timeBuckets, tagNames, tagValues);
      default:
        throw new UnsupportedOperationException("Unsupported aggregation type: " + aggType);
    }
  }

  @Override
  public void init(PinotConfiguration pinotConfiguration) {
  }
}
