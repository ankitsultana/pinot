/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.tsdb.planner;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.pinot.tsdb.spi.AggInfo;
import org.apache.pinot.tsdb.spi.plan.BaseTimeSeriesPlanNode;
import org.apache.pinot.tsdb.spi.plan.LeafTimeSeriesPlanNode;


public class TimeSeriesPlanFragmenter {
  private TimeSeriesPlanFragmenter() {
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

  private static class Context {
    private final List<BaseTimeSeriesPlanNode> _fragments = new ArrayList<>();
  }
}
