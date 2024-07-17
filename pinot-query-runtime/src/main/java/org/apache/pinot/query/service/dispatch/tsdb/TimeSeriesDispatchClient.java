package org.apache.pinot.query.service.dispatch.tsdb;

import io.grpc.Deadline;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.function.Consumer;
import org.apache.pinot.common.proto.PinotQueryWorkerGrpc;
import org.apache.pinot.common.proto.Worker;
import org.apache.pinot.query.routing.QueryServerInstance;


public class TimeSeriesDispatchClient {
  private final ManagedChannel _channel;
  private final PinotQueryWorkerGrpc.PinotQueryWorkerStub _dispatchStub;

  public TimeSeriesDispatchClient(String host, int port) {
    _channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
    _dispatchStub = PinotQueryWorkerGrpc.newStub(_channel);
  }

  public ManagedChannel getChannel() {
    return _channel;
  }

  public void submit(Worker.TimeSeriesQueryRequest request, QueryServerInstance virtualServer, Deadline deadline,
      Consumer<AsyncQueryTimeSeriesDispatchResponse> callback) {
    _dispatchStub.withDeadline(deadline).submitTimeSeries(
        request, new TimeSeriesDispatchObserver(virtualServer, callback));
  }
}
