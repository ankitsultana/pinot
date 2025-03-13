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
package org.apache.pinot.query.planner.logical.rel2plan;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;


public class LeafStageBoundaryComputer {
  private final PlanIdGenerator _idGenerator;
  private final Map<Integer, Integer> _inputToCallerNodeId = new HashMap<>();
  private final Map<Integer, PRelNode> _nodeIdToWrappedRelNode = new HashMap<>();
  private final Set<PRelNode> _leafPlanNodes = new HashSet<>();
  private final Set<Integer> _leafPlanIds = new HashSet<>();
  private final Set<Integer> _leafBoundaryPlanIds = new HashSet<>();

  public LeafStageBoundaryComputer(PlanIdGenerator idGenerator) {
    _idGenerator = idGenerator;
  }

  public PRelNode compute(PRelNode pRelNode) {
    precompute(pRelNode);
    return visit(pRelNode);
  }

  private PRelNode visit(PRelNode pRelNode) {
    List<PRelNode> newInputs = new ArrayList<>();
    for (PRelNode input : pRelNode.getInputs()) {
      newInputs.add(visit(input));
    }
    return new PRelNode(_idGenerator.get(), pRelNode.getRelNode(), pRelNode.getPinotDataDistribution(),
        newInputs, _leafPlanIds.contains(pRelNode.getNodeId()), _leafBoundaryPlanIds.contains(pRelNode.getNodeId()));
  }

  private void precompute(PRelNode pRelNode) {
    precomputeLeafPlanNodesAndNodeIdMap(pRelNode);
    precomputeLeafStageAndBoundary();
  }

  private void precomputeLeafPlanNodesAndNodeIdMap(PRelNode pRelNode) {
    _nodeIdToWrappedRelNode.put(pRelNode.getNodeId(), pRelNode);
    if (pRelNode.getInputs().isEmpty()) {
      _leafPlanNodes.add(pRelNode);
    }
    for (PRelNode input : pRelNode.getInputs()) {
      _inputToCallerNodeId.put(input.getNodeId(), pRelNode.getNodeId());
      precomputeLeafPlanNodesAndNodeIdMap(input);
    }
  }

  private void precomputeLeafStageAndBoundary() {
    for (PRelNode leafPlanNode : _leafPlanNodes) {
      Preconditions.checkState(leafPlanNode.getRelNode() instanceof TableScan,
          "only support table scan in leaf right now");
      PRelNode currentNode = leafPlanNode;
      List<PRelNode> currentLeafStage = new ArrayList<>();
      int projectCount = 0;
      int filterCount = 0;
      while (true) {
        currentLeafStage.add(currentNode);
        if (!_inputToCallerNodeId.containsKey(currentNode.getNodeId())) {
          // Reached root node.
          break;
        }
        currentNode = _nodeIdToWrappedRelNode.get(_inputToCallerNodeId.get(currentNode.getNodeId()));
        if (currentNode.getRelNode() instanceof Project) {
          if (projectCount > 0) {
            break;
          }
          projectCount++;
        } else if (currentNode.getRelNode() instanceof Filter) {
          if (filterCount > 0) {
            break;
          }
          filterCount++;
        } else {
          break;
        }
      }
      currentLeafStage.forEach(x -> _leafPlanIds.add(x.getNodeId()));
      _leafBoundaryPlanIds.add(currentLeafStage.get(currentLeafStage.size() - 1).getNodeId());
    }
  }
}
