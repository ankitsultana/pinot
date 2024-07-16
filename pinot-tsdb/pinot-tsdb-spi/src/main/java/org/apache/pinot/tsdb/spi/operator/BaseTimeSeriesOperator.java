package org.apache.pinot.tsdb.spi.operator;

import java.util.List;
import org.apache.pinot.tsdb.spi.series.SeriesBlock;


public abstract class BaseTimeSeriesOperator {
  protected final List<BaseTimeSeriesOperator> _childOperators;

  public BaseTimeSeriesOperator(List<BaseTimeSeriesOperator> childOperators) {
    _childOperators = childOperators;
  }

  public SeriesBlock nextBlock() {
    long startTime = System.currentTimeMillis();
    try {
      return getNextBlock();
    } finally {
      // add stats
    }
  }

  public abstract SeriesBlock getNextBlock();

  public abstract String getExplainName();

  public List<BaseTimeSeriesOperator> getChildOperators() {
    return _childOperators;
  }
}
