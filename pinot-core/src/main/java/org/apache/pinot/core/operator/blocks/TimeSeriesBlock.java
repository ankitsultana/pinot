package org.apache.pinot.core.operator.blocks;

import org.apache.pinot.core.common.Block;
import org.apache.pinot.tsdb.spi.series.SeriesBlock;


public class TimeSeriesBlock implements Block {
  private final SeriesBlock _seriesBlock;

  public TimeSeriesBlock(SeriesBlock seriesBlock) {
    _seriesBlock = seriesBlock;
  }

  public SeriesBlock getSeriesBlock() {
    return _seriesBlock;
  }
}
