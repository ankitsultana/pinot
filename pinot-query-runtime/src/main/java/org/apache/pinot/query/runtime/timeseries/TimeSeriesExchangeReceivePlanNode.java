package org.apache.pinot.query.runtime.timeseries;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import javax.annotation.Nullable;
import org.apache.pinot.tsdb.spi.AggInfo;
import org.apache.pinot.tsdb.spi.operator.BaseTimeSeriesOperator;
import org.apache.pinot.tsdb.spi.plan.BaseTimeSeriesPlanNode;
import org.apache.pinot.tsdb.spi.series.TimeSeriesBuilderFactory;


public class TimeSeriesExchangeReceivePlanNode extends BaseTimeSeriesPlanNode {
  private final long _deadlineMs;
  private final AggInfo _aggInfo;
  private final TimeSeriesBuilderFactory _factory;
  private BlockingQueue<Object> _receiver;
  private int _numExpectedBlocks;

  public TimeSeriesExchangeReceivePlanNode(String id, List<BaseTimeSeriesPlanNode> children, long deadlineMs,
      @Nullable AggInfo aggInfo, TimeSeriesBuilderFactory factory) {
    super(id, children);
    _deadlineMs = deadlineMs;
    _aggInfo = aggInfo;
    _factory = factory;
  }

  public void init(BlockingQueue<Object> receiver, int numExpectedBlocks) {
    _receiver = receiver;
    _numExpectedBlocks = numExpectedBlocks;
  }

  @Override
  public BaseTimeSeriesPlanNode withChildNodes(List<BaseTimeSeriesPlanNode> newChildNodes) {
    return new TimeSeriesExchangeReceivePlanNode(_id, newChildNodes, _deadlineMs, _aggInfo, _factory);
  }

  @Override
  public String getKlass() {
    return TimeSeriesExchangeReceivePlanNode.class.getName();
  }

  @Override
  public String getExplainName() {
    return "TIME_SERIES_EXCHANGE_RECEIVE";
  }

  @Override
  public BaseTimeSeriesOperator run() {
    return new TimeSeriesExchangeReceiveOperator( _children.get(0).run(), _receiver, _deadlineMs, _numExpectedBlocks,
        _aggInfo, _factory);
  }
}
