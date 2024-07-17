package org.apache.pinot.tsdb.planner.physical;

import org.apache.pinot.core.transport.ServerInstance;


public class TimeSeriesQueryServerInstance {
  private final String _hostname;
  private final int _queryServicePort;
  private final int _queryMailboxPort;

  public TimeSeriesQueryServerInstance(ServerInstance serverInstance) {
    this(serverInstance.getHostname(), serverInstance.getQueryServicePort(), serverInstance.getQueryMailboxPort());
  }

  public TimeSeriesQueryServerInstance(String hostname, int queryServicePort, int queryMailboxPort) {
    _hostname = hostname;
    _queryServicePort = queryServicePort;
    _queryMailboxPort = queryMailboxPort;
  }

  public String getHostname() {
    return _hostname;
  }

  public int getQueryServicePort() {
    return _queryServicePort;
  }

  public int getQueryMailboxPort() {
    return _queryMailboxPort;
  }
}
