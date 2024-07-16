package org.apache.pinot.tsdb.spi.operator;

import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.tsdb.spi.TimeBuckets;
import org.apache.pinot.tsdb.spi.series.SeriesBlock;


public abstract class BasePipelineOperator extends BaseTimeSeriesOperator {
  protected final List<? extends BaseTimeSeriesOperator> _childOperators;

  protected BasePipelineOperator(List<? extends BaseTimeSeriesOperator> childOperators) {
    super((List<BaseTimeSeriesOperator>) childOperators);
    _childOperators = childOperators;
  }

  public abstract SeriesBlock merge(List<SeriesBlock> allBlocks);

  @Override
  public SeriesBlock getNextBlock() {
    List<SeriesBlock> inputBlocks = new ArrayList<>(_childOperators.size());
    for (BaseTimeSeriesOperator pipelineOperator : _childOperators) {
      inputBlocks.add(pipelineOperator.nextBlock());
    }
    return merge(inputBlocks);
  }

  protected TimeBuckets validateTimeBuckets(List<SeriesBlock> allBlocks) {
    TimeBuckets result = null;
    for (SeriesBlock seriesBlock : allBlocks) {
      if (result == null) {
        result = seriesBlock.getTimeBuckets();
      } else {
        if (!result.equals(seriesBlock.getTimeBuckets())) {
          throw new IllegalStateException("Attempting to merge pipelines with different time buckets");
        }
      }
    }
    return result;
  }
}
