package org.apache.pinot.core.operator.combine;

import java.util.List;
import java.util.concurrent.ExecutorService;
import javax.annotation.Nullable;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.blocks.results.TimeSeriesResultsBlock;
import org.apache.pinot.core.operator.combine.merger.ResultsBlockMerger;
import org.apache.pinot.core.query.request.context.QueryContext;


public class TimeSeriesCombineOperator extends BaseSingleBlockCombineOperator<TimeSeriesResultsBlock> {
  private static final String EXPLAIN_NAME = "TIME_SERIES_COMBINE";

  public TimeSeriesCombineOperator(ResultsBlockMerger<TimeSeriesResultsBlock> resultsBlockMerger,
      List<Operator> operators, QueryContext queryContext, ExecutorService executorService) {
    super(resultsBlockMerger, operators, queryContext, executorService);
  }

  @Nullable
  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }
}
