package org.apache.pinot.tsdb.spi;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;


public class AggInfo {
  private final String _aggFunction;
  private final boolean _isPartial;

  @JsonCreator
  public AggInfo(
      @JsonProperty("aggFunction") String aggFunction,
      @JsonProperty("isPartial") boolean isPartial) {
    _aggFunction = aggFunction;
    _isPartial = isPartial;
  }

  public String getAggFunction() {
    return _aggFunction;
  }

  public boolean isPartial() {
    return _isPartial;
  }
}
