package org.apache.pinot.core.plan;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.TimeSeriesContext;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.BaseProjectOperator;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.blocks.results.TimeSeriesResultsBlock;
import org.apache.pinot.core.operator.timeseries.TimeSeriesAggregationOperator;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.SegmentContext;
import org.apache.pinot.tsdb.spi.series.SeriesBuilderFactory;


public class TimeSeriesPlanNode implements PlanNode {
  private final SegmentContext _segmentContext;
  private final QueryContext _queryContext;
  private final TimeSeriesContext _timeSeriesContext;
  private final SeriesBuilderFactory _seriesBuilderFactory;

  public TimeSeriesPlanNode(SegmentContext segmentContext, QueryContext queryContext,
      SeriesBuilderFactory seriesBuilderFactory) {
    _segmentContext = segmentContext;
    _queryContext = queryContext;
    _timeSeriesContext = queryContext.getTimeSeriesContext();
    _seriesBuilderFactory = seriesBuilderFactory;
  }

  @Override
  public Operator<TimeSeriesResultsBlock> run() {
    FilterPlanNode filterPlanNode = new FilterPlanNode(_segmentContext, _queryContext);
    ProjectPlanNode projectPlanNode = new ProjectPlanNode(
        _segmentContext, _queryContext, getProjectPlanNodeExpressions(), DocIdSetPlanNode.MAX_DOC_PER_CALL,
        filterPlanNode.run());
    BaseProjectOperator<? extends ValueBlock> projectionOperator = projectPlanNode.run();
    return new TimeSeriesAggregationOperator(
        _timeSeriesContext.getTimeColumn(),
        _timeSeriesContext.getOffsetSeconds(),
        _timeSeriesContext.getAggType(),
        _timeSeriesContext.getValueExpression(),
        getGroupByColumns(),
        null /* TODO: Need to pass this */,
        projectionOperator,
        _seriesBuilderFactory);
  }

  private List<ExpressionContext> getProjectPlanNodeExpressions() {
    List<ExpressionContext> result = new ArrayList<>(_queryContext.getSelectExpressions());
    if (CollectionUtils.isNotEmpty(_queryContext.getGroupByExpressions())) {
      result.addAll(_queryContext.getGroupByExpressions());
    }
    return result;
  }

  private List<String> getGroupByColumns() {
    if (_queryContext.getGroupByExpressions() == null) {
      return new ArrayList<>();
    }
    List<String> groupByColumns = new ArrayList<>();
    for (ExpressionContext expression : _queryContext.getGroupByExpressions()) {
      Preconditions.checkState(expression.getType() == ExpressionContext.Type.IDENTIFIER);
      groupByColumns.add(expression.getIdentifier());
    }
    return groupByColumns;
  }
}
