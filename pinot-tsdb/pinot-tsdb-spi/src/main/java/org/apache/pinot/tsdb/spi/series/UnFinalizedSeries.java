package org.apache.pinot.tsdb.spi.series;

import java.time.Duration;
import java.util.List;
import org.apache.pinot.tsdb.spi.TimeBuckets;


public class UnFinalizedSeries extends Series {
  private final Object[] _objectValues;

  public UnFinalizedSeries(String id, Long[] timeValues, TimeBuckets timeBuckets, Object[] values, List<String> tagNames,
      Object[] tagValues) {
    super(id, timeValues, timeBuckets, null, tagNames, tagValues);
    _objectValues = values;
  }

  public Double[] getValues() {
    throw new UnsupportedOperationException("Unfinalized series does not support getValues()");
  }

  public Object[] getObjectValues() {
    return _objectValues;
  }
}
