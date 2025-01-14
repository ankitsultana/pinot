package org.apache.pinot.query.planner.logical.rel2plan;

import org.apache.pinot.calcite.rel.PinotDataDistribution;
import org.apache.pinot.query.planner.plannode.PlanNode;


public class WrappedPlanNode {
  private final long _nodeId;
  private final PlanNode _planNode;
  private final PinotDataDistribution _pinotDataDistribution;

  public WrappedPlanNode(long nodeId, PlanNode planNode, PinotDataDistribution pinotDataDistribution) {
    _nodeId = nodeId;
    _planNode = planNode;
    _pinotDataDistribution = pinotDataDistribution;
  }

  public long getNodeId() {
    return _nodeId;
  }

  public PlanNode getPlanNode() {
    return _planNode;
  }

  public PinotDataDistribution getPinotDataDistribution() {
    return _pinotDataDistribution;
  }
}
