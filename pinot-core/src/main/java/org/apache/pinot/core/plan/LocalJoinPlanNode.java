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
package org.apache.pinot.core.plan;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import org.apache.pinot.core.operator.combine.BaseCombineOperator;
import org.apache.pinot.core.operator.combine.LocalJoinOperator;
import org.apache.pinot.core.operator.transform.TransformOperator;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.spi.trace.InvocationScope;
import org.apache.pinot.spi.trace.Tracing;


public class LocalJoinPlanNode extends CombinePlanNode {
  private final List<TransformPlanNode> _leftPlanNodes;
  private final List<TransformPlanNode> _rightPlanNodes;
  private final ExecutorService _executorService;
  private final QueryContext _queryContext;

  public LocalJoinPlanNode(List<TransformPlanNode> leftPlanNodes, List<TransformPlanNode> rightPlanNodes,
      ExecutorService executorService, QueryContext queryContext) {
    super(Collections.emptyList(), queryContext, executorService, null);
    _leftPlanNodes = leftPlanNodes;
    _rightPlanNodes = rightPlanNodes;
    _executorService = executorService;
    _queryContext = queryContext;
  }

  @Override
  public BaseCombineOperator run() {
    try (InvocationScope ignored = Tracing.getTracer().createScope(CombinePlanNode.class)) {
      return getCombineOperator();
    }
  }

  private BaseCombineOperator getCombineOperator() {
    List<TransformOperator> leftOperators = new ArrayList<>(_leftPlanNodes.size());
    for (int i = 0; i < _leftPlanNodes.size(); i++) {
      leftOperators.add(_leftPlanNodes.get(i).run());
    }
    List<TransformOperator> rightOperators = new ArrayList<>(_rightPlanNodes.size());
    for (int i = 0; i < _rightPlanNodes.size(); i++) {
      rightOperators.add(_rightPlanNodes.get(i).run());
    }
    return new LocalJoinOperator(leftOperators, rightOperators, _leftPlanNodes.get(0).getExpressions(),
        _rightPlanNodes.get(0).getExpressions(), _queryContext, _executorService);
  }
}
