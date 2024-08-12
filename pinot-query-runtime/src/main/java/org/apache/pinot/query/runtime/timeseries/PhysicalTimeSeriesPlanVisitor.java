package org.apache.pinot.query.runtime.timeseries;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FilterContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.common.request.context.TimeSeriesContext;
import org.apache.pinot.core.query.executor.QueryExecutor;
import org.apache.pinot.core.query.request.ServerQueryRequest;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.apache.pinot.tsdb.spi.operator.BaseTimeSeriesOperator;
import org.apache.pinot.tsdb.spi.plan.BaseTimeSeriesPlanNode;
import org.apache.pinot.tsdb.spi.plan.ScanFilterAndProjectPlanNode;


public class PhysicalTimeSeriesPlanVisitor {
  public static final PhysicalTimeSeriesPlanVisitor INSTANCE = new PhysicalTimeSeriesPlanVisitor();

  private QueryExecutor _queryExecutor;
  private ExecutorService _executorService;
  private ServerMetrics _serverMetrics;

  private PhysicalTimeSeriesPlanVisitor() {
  }

  public void init(QueryExecutor queryExecutor, ExecutorService executorService, ServerMetrics serverMetrics) {
    _queryExecutor = queryExecutor;
    _executorService = executorService;
    _serverMetrics = serverMetrics;
  }

  public BaseTimeSeriesOperator compile(BaseTimeSeriesPlanNode rootNode, TimeSeriesExecutionContext context) {
    // Step-1: Replace scan filter project with our physical plan node with Pinot Core and Runtime context
    initLeafPlanNode(rootNode, context);
    // Step-2: Trigger recursive operator generation
    return rootNode.run();
  }

  public void initLeafPlanNode(BaseTimeSeriesPlanNode planNode, TimeSeriesExecutionContext context) {
    for (int index = 0; index < planNode.getChildren().size(); index++) {
      BaseTimeSeriesPlanNode childNode = planNode.getChildren().get(index);
      if (childNode instanceof ScanFilterAndProjectPlanNode) {
        ScanFilterAndProjectPlanNode sfpNode = (ScanFilterAndProjectPlanNode) planNode;
        List<String> segments = context.getPlanIdToSegmentsMap().get(sfpNode.getId());
        ServerQueryRequest serverQueryRequest = compileLeafServerQueryRequest(sfpNode, segments, context);
        TimeSeriesPhysicalTableScan physicalTableScan = new TimeSeriesPhysicalTableScan(planNode.getId(),
            serverQueryRequest, _queryExecutor, _executorService);
        planNode.getChildren().set(index, physicalTableScan);
      } else {
        initLeafPlanNode(childNode, context);
      }
    }
  }

  public ServerQueryRequest compileLeafServerQueryRequest(ScanFilterAndProjectPlanNode sfpNode, List<String> segments,
      TimeSeriesExecutionContext context) {
    return new ServerQueryRequest(compileQueryContext(sfpNode, context),
        segments, /* TODO: Pass metadata from request */ Collections.emptyMap(), _serverMetrics);
  }

  public QueryContext compileQueryContext(ScanFilterAndProjectPlanNode sfpNode, TimeSeriesExecutionContext context) {
    FilterContext filterContext =
        RequestContextUtils.getFilter(CalciteSqlParser.compileToExpression(sfpNode.getFilterExpression()));
    List<ExpressionContext> groupByExpressions = sfpNode.getGroupByColumns().stream()
        .map(ExpressionContext::forIdentifier).collect(Collectors.toList());
    ExpressionContext valueExpression = RequestContextUtils.getExpression(sfpNode.getValueExpression());
    TimeSeriesContext timeSeriesContext = new TimeSeriesContext(context.getEngine(),
        sfpNode.getTimeColumn(),
        sfpNode.getTimeUnit(), context.getInitialTimeBuckets(), sfpNode.getOffset(),
        valueExpression,
        sfpNode.getAggInfo());
    return new QueryContext.Builder()
        .setTableName(sfpNode.getTableName())
        .setFilter(filterContext)
        .setGroupByExpressions(groupByExpressions)
        .setSelectExpressions(Collections.emptyList())
        .setQueryOptions(Collections.emptyMap())
        .setAliasList(Collections.emptyList())
        .setTimeSeriesContext(timeSeriesContext)
        .build();
  }
}
