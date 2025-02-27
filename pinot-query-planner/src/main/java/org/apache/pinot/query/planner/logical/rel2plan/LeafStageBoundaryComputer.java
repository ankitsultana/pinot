package org.apache.pinot.query.planner.logical.rel2plan;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;


public class LeafStageBoundaryComputer {
  private final Map<Integer, Integer> _inputToCallerNodeId = new HashMap<>();
  private final Map<Integer, WrappedRelNode> _nodeIdToWrappedRelNode = new HashMap<>();
  private final Set<WrappedRelNode> _leafPlanNodes = new HashSet<>();

  public LeafStageBoundaryComputer() {
  }

  public void compute(WrappedRelNode wrappedRelNode) {
    precompute(wrappedRelNode, null);
    for (WrappedRelNode leafPlanNode : _leafPlanNodes) {
      Preconditions.checkState(leafPlanNode.getRelNode() instanceof TableScan, "only support table scan in leaf right now");
      WrappedRelNode currentNode = leafPlanNode;
      List<WrappedRelNode> currentLeafStage = new ArrayList<>();
      int projectCount = 0;
      int filterCount = 0;
      while (true) {
        currentLeafStage.add(currentNode);
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
      currentLeafStage.forEach(x -> x.setLeafStage(true));
      currentLeafStage.get(currentLeafStage.size() - 1).setLeafStageBoundary(true);
    }
  }

  private void precompute(WrappedRelNode wrappedRelNode, @Nullable WrappedRelNode callerRelNode) {
    _nodeIdToWrappedRelNode.put(wrappedRelNode.getNodeId(), wrappedRelNode);
    if (wrappedRelNode.getInputs().isEmpty()) {
      _leafPlanNodes.add(wrappedRelNode);
    }
    for (WrappedRelNode input : wrappedRelNode.getInputs()) {
      _inputToCallerNodeId.put(input.getNodeId(), wrappedRelNode.getNodeId());
      precompute(input, wrappedRelNode);
    }
  }
}
