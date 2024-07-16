package org.apache.pinot.tsdb.spi.series;

import java.util.List;
import org.apache.pinot.tsdb.spi.TimeBuckets;


public abstract class BaseSeriesBuilder {
  protected final String _id;
  protected final TimeBuckets _timeBuckets;
  protected final List<String> _tagNames;
  protected final List<String> _tagValues;

  public BaseSeriesBuilder(String id, TimeBuckets timeBuckets, List<String> tagNames, List<String> tagValues) {
    _id = id;
    _timeBuckets = timeBuckets;
    _tagNames = tagNames;
    _tagValues = tagValues;
  }

  public abstract void addValueAtIndex(int timeBucketIndex, Double value);

  public void addValueAtIndex(int timeBucketIndex, String value) {
    throw new IllegalStateException("This aggregation function does not support string input");
  }

  public abstract void addValue(long timeValue, Double value);

  public void mergeSeries(Series series) {
    int numDataPoints = series.getTimeValues().length;
    for (int i = 0; i < numDataPoints; i++) {
      addValue(series.getTimeValues()[i], series.getValues()[i]);
    }
  }

  public void mergeAlignedSeries(Series series) {
    int numDataPoints = series.getTimeValues().length;
    for (int i = 0; i < numDataPoints; i++) {
      addValueAtIndex(i, series.getValues()[i]);
    }
  }

  public abstract Series build();
}
