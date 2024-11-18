package org.apache.pinot.tsdb.planner;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.pinot.tsdb.spi.AggInfo;
import org.apache.pinot.tsdb.spi.plan.BaseTimeSeriesPlanNode;
import org.apache.pinot.tsdb.spi.plan.LeafTimeSeriesPlanNode;


public class TimeSeriesPlanFragmenter {
  public TimeSeriesPlanFragmenter() {
  }

  public static List<BaseTimeSeriesPlanNode> getFragments(BaseTimeSeriesPlanNode rootNode, boolean isSingleNodeQuery) {
    List<BaseTimeSeriesPlanNode> result = new ArrayList<>();
    Context context = new Context();
    if (isSingleNodeQuery) {
      final String id = rootNode.getId();
      return ImmutableList.of(new TimeSeriesExchangeNode(id, Collections.emptyList(), null), rootNode);
    }
    result.add(fragment(rootNode, context));
    result.addAll(context._fragments);
    return result;
  }

  private static BaseTimeSeriesPlanNode fragment(BaseTimeSeriesPlanNode planNode, Context context) {
    if (planNode instanceof LeafTimeSeriesPlanNode) {
      Preconditions.checkState(visitServerFragment(planNode) == 1, "Found multiple exchange node in hierarchy. Only 1 allowed.");
      LeafTimeSeriesPlanNode newLeafPlanNode = cloneWithPartialAgg((LeafTimeSeriesPlanNode) planNode);
      context._fragments.add(newLeafPlanNode);
      return new TimeSeriesExchangeNode(planNode.getId(), Collections.emptyList(), null);
    }
    List<BaseTimeSeriesPlanNode> newChildNodes = new ArrayList<>();
    for (BaseTimeSeriesPlanNode childNode : planNode.getChildren()) {
      newChildNodes.add(fragment(childNode, context));
    }
    return planNode.withChildNodes(newChildNodes);
  }

  private static LeafTimeSeriesPlanNode cloneWithPartialAgg(LeafTimeSeriesPlanNode planNode) {
    AggInfo newAggInfo = new AggInfo(planNode.getAggInfo().getAggFunction(), true,
        planNode.getAggInfo().getParams());
    return new LeafTimeSeriesPlanNode(planNode.getId(),
        planNode.getChildren(), planNode.getTableName(), planNode.getTimeColumn(),
        planNode.getTimeUnit(), planNode.getOffsetSeconds(), planNode.getFilterExpression(),
        planNode.getValueExpression(), newAggInfo, planNode.getGroupByExpressions());
  }

  private static int visitServerFragment(BaseTimeSeriesPlanNode planNode) {
    int count = 0;
    if (planNode instanceof TimeSeriesExchangeNode) {
      count++;
    }
    for (BaseTimeSeriesPlanNode childNode : planNode.getChildren()) {
      count += visitServerFragment(childNode);
    }
    return count;
  }

  private static class Context {
    private final List<BaseTimeSeriesPlanNode> _fragments = new ArrayList<>();
  }
}
