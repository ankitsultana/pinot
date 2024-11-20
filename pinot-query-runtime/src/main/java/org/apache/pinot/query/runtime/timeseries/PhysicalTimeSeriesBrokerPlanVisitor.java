package org.apache.pinot.query.runtime.timeseries;

import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import org.apache.pinot.tsdb.planner.TimeSeriesExchangeNode;
import org.apache.pinot.tsdb.spi.operator.BaseTimeSeriesOperator;
import org.apache.pinot.tsdb.spi.plan.BaseTimeSeriesPlanNode;
import org.apache.pinot.tsdb.spi.plan.LeafTimeSeriesPlanNode;


public class PhysicalTimeSeriesBrokerPlanVisitor {
  // Warning: Don't use singleton access pattern, since Quickstarts run in a single JVM and spawn multiple broker/server
  public PhysicalTimeSeriesBrokerPlanVisitor() {
  }

  public void init() {
  }

  public BaseTimeSeriesOperator compile(BaseTimeSeriesPlanNode rootNode, TimeSeriesExecutionContext context) {
    // Step-1: Replace scan filter project with our physical plan node with Pinot Core and Runtime context
    rootNode = initExchangeReceivePlanNode(rootNode, context);
    // Step-2: Trigger recursive operator generation
    return rootNode.run();
  }

  public BaseTimeSeriesPlanNode initExchangeReceivePlanNode(BaseTimeSeriesPlanNode planNode, TimeSeriesExecutionContext context) {
    if (planNode instanceof LeafTimeSeriesPlanNode) {
      throw new IllegalStateException("Found leaf time series plan node in broker");
    } else if (planNode instanceof TimeSeriesExchangeNode) {
      return compileToPhysicalReceiveNode((TimeSeriesExchangeNode) planNode, context);
    }
    for (int index = 0; index < planNode.getChildren().size(); index++) {
      BaseTimeSeriesPlanNode childNode = planNode.getChildren().get(index);
      if (childNode instanceof TimeSeriesExchangeNode) {
        TimeSeriesExchangeReceivePlanNode exchangeReceivePlanNode = compileToPhysicalReceiveNode(
            (TimeSeriesExchangeNode) childNode, context);
        planNode.getChildren().set(index, exchangeReceivePlanNode);
      } else {
        initExchangeReceivePlanNode(childNode, context);
      }
    }
    return planNode;
  }

  TimeSeriesExchangeReceivePlanNode compileToPhysicalReceiveNode(TimeSeriesExchangeNode exchangeNode,
      TimeSeriesExecutionContext context) {
    TimeSeriesExchangeReceivePlanNode exchangeReceivePlanNode = new TimeSeriesExchangeReceivePlanNode(
        exchangeNode.getId(), context.getDeadlineMs(), exchangeNode.getAggInfo(), context.getSeriesBuilderFactory());
    BlockingQueue<Object> receiver = context.getReceiverByPlanId().get(exchangeNode.getId());
    exchangeReceivePlanNode.init(Objects.requireNonNull(receiver, "No receiver for node"), context.getNumQueryServers());
    return exchangeReceivePlanNode;
  }
}
