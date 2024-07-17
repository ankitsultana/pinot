package org.apache.pinot.core.operator.timeseries;

import java.util.Collections;
import org.apache.pinot.core.operator.blocks.results.TimeSeriesResultsBlock;
import org.apache.pinot.core.operator.combine.TimeSeriesCombineOperator;
import org.apache.pinot.tsdb.spi.operator.BaseTimeSeriesOperator;
import org.apache.pinot.tsdb.spi.series.SeriesBlock;


/**
 * Adapter operator that ties in the Pinot TimeSeriesCombineOperator with the Pinot BaseTimeSeriesOperator.
 */
public class TimeSeriesPassThroughOperator extends BaseTimeSeriesOperator {
  private static final String EXPLAIN_NAME = "TIME_SERIES_PASS_THROUGH_OPERATOR";
  private final TimeSeriesCombineOperator _timeSeriesCombineOperator;

  public TimeSeriesPassThroughOperator(TimeSeriesCombineOperator combineOperator) {
    super(Collections.emptyList());
    _timeSeriesCombineOperator = combineOperator;
  }

  @Override
  public SeriesBlock getNextBlock() {
    TimeSeriesResultsBlock resultsBlock = (TimeSeriesResultsBlock) _timeSeriesCombineOperator.nextBlock();
    return resultsBlock.getSeriesBlock();
  }

  @Override
  public String getExplainName() {
    return EXPLAIN_NAME;
  }
}
