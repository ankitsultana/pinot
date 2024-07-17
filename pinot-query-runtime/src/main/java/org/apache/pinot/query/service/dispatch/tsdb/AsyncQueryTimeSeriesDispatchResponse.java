package org.apache.pinot.query.service.dispatch.tsdb;

import javax.annotation.Nullable;
import org.apache.pinot.common.proto.Worker;
import org.apache.pinot.query.routing.QueryServerInstance;


public class AsyncQueryTimeSeriesDispatchResponse {
  private final QueryServerInstance _serverInstance;
  private final Worker.TimeSeriesResponse _queryResponse;
  private final Throwable _throwable;

  public AsyncQueryTimeSeriesDispatchResponse(QueryServerInstance serverInstance,
      @Nullable Worker.TimeSeriesResponse queryResponse,
      @Nullable Throwable throwable) {
    _serverInstance = serverInstance;
    _queryResponse = queryResponse;
    _throwable = throwable;
  }

  public QueryServerInstance getServerInstance() {
    return _serverInstance;
  }

  @Nullable
  public Worker.TimeSeriesResponse getQueryResponse() {
    return _queryResponse;
  }

  @Nullable
  public Throwable getThrowable() {
    return _throwable;
  }
}
