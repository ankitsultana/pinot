package org.apache.pinot.tsdb.spi.series;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Objects;


/**
 * A Time Series is a list of pairs of time and data values, where time is stored in increasing order.
 * Further, a time series has a list of pairs of tag names and tag values which identify the series.
 * This identification may not be unique, and there may exist multiple series with the same tag names and values
 * in some execution context. Those semantics are defined by the execution context.
 * <p>
 *   <b>Warning:</b> The time and value arrays passed to the Series are not copied, and can be modified by anyone with
 *   access to them. This is by design, to make it easier to re-use buffers during time-series operations.
 * </p>
 */
public class Series {
  private final String _id;
  private final Long[] _timeValues;
  private final Duration _stepSize;
  private final Double[] _values;
  private final List<String> _tagNames;
  private final List<String> _tagValues;

  public Series(String id, Long[] timeValues, Duration stepSize, Double[] values, List<String> tagNames,
      List<String> tagValues) {
    _id = id;
    _timeValues = timeValues;
    _stepSize = stepSize;
    _values = values;
    _tagNames = Collections.unmodifiableList(tagNames);
    _tagValues = Collections.unmodifiableList(tagValues);
  }

  public String getId() {
    return _id;
  }

  public Long[] getTimeValues() {
    return _timeValues;
  }

  public Duration getStepSize() {
    return _stepSize;
  }

  public Double[] getValues() {
    return _values;
  }

  public List<String> getTagNames() {
    return _tagNames;
  }

  public List<String> getTagValues() {
    return _tagValues;
  }

  public String getTagsSerialized() {
    if (_tagNames.isEmpty()) {
      return "*";
    }
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < _tagNames.size(); i++) {
      if (i > 0) {
        sb.append(",");
      }
      sb.append(String.format("%s=%s", _tagNames.get(i), _tagValues.get(i)));
    }
    return sb.toString();
  }

  // TODO: This can be cleaned up
  public static long hash(Object[] tagNamesAndValues) {
    return Objects.hash(tagNamesAndValues);
  }
}
