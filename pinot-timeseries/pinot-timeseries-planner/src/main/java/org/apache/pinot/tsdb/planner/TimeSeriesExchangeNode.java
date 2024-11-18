package org.apache.pinot.tsdb.planner;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.tsdb.spi.AggInfo;
import org.apache.pinot.tsdb.spi.operator.BaseTimeSeriesOperator;
import org.apache.pinot.tsdb.spi.plan.BaseTimeSeriesPlanNode;


public class TimeSeriesExchangeNode extends BaseTimeSeriesPlanNode {
  @Nullable
  private final AggInfo _aggInfo;

  @JsonCreator
  public TimeSeriesExchangeNode(@JsonProperty("id") String id,
      @JsonProperty("children") List<BaseTimeSeriesPlanNode> children,
      @Nullable @JsonProperty("aggInfo") AggInfo aggInfo) {
    super(id, children);
    _aggInfo = aggInfo;
  }

  @Nullable
  public AggInfo getAggInfo() {
    return _aggInfo;
  }

  @Override
  public BaseTimeSeriesPlanNode withChildNodes(List<BaseTimeSeriesPlanNode> newChildNodes) {
    return new TimeSeriesExchangeNode(_id, newChildNodes, _aggInfo);
  }

  @Override
  public String getKlass() {
    return TimeSeriesExchangeNode.class.getName();
  }

  @Override
  public String getExplainName() {
    return "TIME_SERIES_BROKER_RECEIVE";
  }

  @Override
  public BaseTimeSeriesOperator run() {
    throw new IllegalStateException("Time Series Exchange should have been replaced with a physical plan node");
  }
}
