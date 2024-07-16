package org.apache.pinot.tsdb.spi.series;

import java.time.Duration;
import java.util.List;


public class UnFinalizedSeries extends Series {
  private final Object[] _objectValues;

  public UnFinalizedSeries(String id, Long[] timeValues, Duration stepSize, Object[] values, List<String> tagNames,
      List<String> tagValues) {
    super(id, timeValues, stepSize, null, tagNames, tagValues);
    _objectValues = values;
  }

  public Double[] getValues() {
    throw new UnsupportedOperationException("Unfinalized series does not support getValues()");
  }

  public Object[] getObjectValues() {
    return _objectValues;
  }
}
