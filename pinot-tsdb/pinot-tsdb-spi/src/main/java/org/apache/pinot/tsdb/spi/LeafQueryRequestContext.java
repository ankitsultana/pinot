package org.apache.pinot.tsdb.spi;

import java.util.List;


public class LeafQueryRequestContext {
  private final long _requestId;
  private final String _brokerId;
  private final boolean _enableTrace;
  private final boolean _enableStreaming;
  private final List<String> _segmentsToQuery;
  private final List<String> _optionalSegments;

  public LeafQueryRequestContext(long requestId, String brokerId, boolean enableTrace, boolean enableStreaming, List<String> segmentsToQuery, List<String> optionalSegments) {
    _requestId = requestId;
    _brokerId = brokerId;
    _enableTrace = enableTrace;
    _enableStreaming = enableStreaming;
    _segmentsToQuery = segmentsToQuery;
    _optionalSegments = optionalSegments;
  }

  public long getRequestId() {
    return _requestId;
  }

  public String getBrokerId() {
    return _brokerId;
  }

  public boolean isEnableTrace() {
    return _enableTrace;
  }

  public boolean isEnableStreaming() {
    return _enableStreaming;
  }

  public List<String> getSegmentsToQuery() {
    return _segmentsToQuery;
  }

  public List<String> getOptionalSegments() {
    return _optionalSegments;
  }
}
