package org.apache.pinot.core.operator.timeseries.executor;

import org.apache.pinot.core.operator.blocks.TimeSeriesBuilderBlock;
import org.apache.pinot.core.operator.blocks.ValueBlock;


public interface TimeSeriesGroupByExecutor {
  void process(int numDocs, int[] timeValueIndexes, ValueBlock valueBlock);

  TimeSeriesBuilderBlock getTimeSeriesBuilderBlock();
}
