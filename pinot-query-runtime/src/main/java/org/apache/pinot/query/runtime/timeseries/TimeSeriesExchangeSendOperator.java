package org.apache.pinot.query.runtime.timeseries;

import java.util.List;
import org.apache.pinot.tsdb.spi.operator.BaseTimeSeriesOperator;
import org.apache.pinot.tsdb.spi.series.TimeSeriesBlock;


public class TimeSeriesExchangeSendOperator extends BaseTimeSeriesOperator {
  public TimeSeriesExchangeSendOperator(List<BaseTimeSeriesOperator> childOperators) {
    super(childOperators);
  }

  @Override
  public TimeSeriesBlock getNextBlock() {
    return _childOperators.get(0).nextBlock();
  }

  @Override
  public String getExplainName() {
    return "TIME_SERIES_EXCHANGE_SEND";
  }
}
