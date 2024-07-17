package org.apache.pinot.core.operator.timeseries;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.operator.blocks.results.TimeSeriesResultsBlock;
import org.apache.pinot.core.operator.transform.TransformOperator;
import org.apache.pinot.tsdb.spi.series.SeriesBuilderFactory;
import org.jetbrains.annotations.Nullable;


public class TimeSeriesSelectionOperator extends BaseOperator<TimeSeriesResultsBlock> {
  private static final String EXPLAIN_NAME = "TIME_SERIES_SELECTION_OPERATOR";
  private final Long _evaluationTimestamp;
  private final TransformOperator _transformOperator;
  private final SeriesBuilderFactory _seriesBuilderFactory;

  public TimeSeriesSelectionOperator(Long evaluationTimestamp,
      TransformOperator transformOperator, SeriesBuilderFactory seriesBuilderFactory) {
    _evaluationTimestamp = evaluationTimestamp;
    _transformOperator = transformOperator;
    _seriesBuilderFactory = seriesBuilderFactory;
  }

  @Override
  protected TimeSeriesResultsBlock getNextBlock() {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public List<? extends Operator> getChildOperators() {
    return ImmutableList.of(_transformOperator);
  }

  @Nullable
  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }
}
