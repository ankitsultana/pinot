package org.apache.pinot.query.service.dispatch.tsdb;

import io.grpc.stub.StreamObserver;
import java.util.function.Consumer;
import org.apache.pinot.common.proto.Worker;
import org.apache.pinot.query.routing.QueryServerInstance;


class TimeSeriesDispatchObserver implements StreamObserver<Worker.TimeSeriesResponse> {
  private final QueryServerInstance _serverInstance;
  private final Consumer<AsyncQueryTimeSeriesDispatchResponse> _callback;

  private Worker.TimeSeriesResponse _timeSeriesResponse;

  public TimeSeriesDispatchObserver(QueryServerInstance serverInstance, Consumer<AsyncQueryTimeSeriesDispatchResponse> callback) {
    _serverInstance = serverInstance;
    _callback = callback;
  }

  @Override
  public void onNext(Worker.TimeSeriesResponse timeSeriesResponse) {
    _timeSeriesResponse = timeSeriesResponse;
  }

  @Override
  public void onError(Throwable throwable) {
    _callback.accept(
        new AsyncQueryTimeSeriesDispatchResponse(
            _serverInstance,
            Worker.TimeSeriesResponse.getDefaultInstance(),
            throwable));
  }

  @Override
  public void onCompleted() {
    _callback.accept(
        new AsyncQueryTimeSeriesDispatchResponse(
            _serverInstance,
            _timeSeriesResponse,
            null));
  }
}
