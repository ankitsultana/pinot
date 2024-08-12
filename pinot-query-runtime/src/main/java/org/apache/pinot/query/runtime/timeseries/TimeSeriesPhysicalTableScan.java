package org.apache.pinot.query.runtime.timeseries;

import java.util.Collections;
import java.util.concurrent.ExecutorService;
import org.apache.pinot.core.query.executor.QueryExecutor;
import org.apache.pinot.core.query.request.ServerQueryRequest;
import org.apache.pinot.tsdb.spi.operator.BaseTimeSeriesOperator;
import org.apache.pinot.tsdb.spi.plan.BaseTimeSeriesPlanNode;


public class TimeSeriesPhysicalTableScan extends BaseTimeSeriesPlanNode {
  private final ServerQueryRequest _request;
  private final QueryExecutor _queryExecutor;
  private final ExecutorService _executorService;


  public TimeSeriesPhysicalTableScan(
     String id,
     ServerQueryRequest serverQueryRequest,
     QueryExecutor queryExecutor,
     ExecutorService executorService) {
    super(id, Collections.emptyList());
    _request = serverQueryRequest;
    _queryExecutor = queryExecutor;
    _executorService = executorService;
  }

  public ServerQueryRequest getServerQueryRequest() {
    return _request;
  }

  public QueryExecutor getQueryExecutor() {
    return _queryExecutor;
  }

  public ExecutorService getExecutorService() {
    return _executorService;
  }

  public String getKlass() {
    return TimeSeriesPhysicalTableScan.class.getName();
  }

  @Override
  public String getExplainName() {
    return "PHYSICAL_TABLE_SCAN";
  }

  @Override
  public BaseTimeSeriesOperator run() {
    return new LeafTimeSeriesOperator(_request, _queryExecutor, _executorService);
  }
}
