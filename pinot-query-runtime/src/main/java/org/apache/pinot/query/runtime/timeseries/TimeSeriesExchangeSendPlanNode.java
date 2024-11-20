package org.apache.pinot.query.runtime.timeseries;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Collections;
import java.util.List;
import org.apache.pinot.tsdb.spi.operator.BaseTimeSeriesOperator;
import org.apache.pinot.tsdb.spi.plan.BaseTimeSeriesPlanNode;


public class TimeSeriesExchangeSendPlanNode extends BaseTimeSeriesPlanNode {
  @JsonCreator
  public TimeSeriesExchangeSendPlanNode(@JsonProperty("id") String id,
     @JsonProperty("children") List<BaseTimeSeriesPlanNode> children) {
    super(id, children);
  }

  @Override
  public BaseTimeSeriesPlanNode withChildNodes(List<BaseTimeSeriesPlanNode> newChildNodes) {
    return new TimeSeriesExchangeSendPlanNode(_id, newChildNodes);
  }

  @Override
  public String getKlass() {
    return TimeSeriesExchangeSendPlanNode.class.getName();
  }

  @Override
  public String getExplainName() {
    return "TIME_SERIES_EXCHANGE_SEND";
  }

  @Override
  public BaseTimeSeriesOperator run() {
    return new TimeSeriesExchangeSendOperator(Collections.singletonList(_children.get(0).run()));
  }
}
