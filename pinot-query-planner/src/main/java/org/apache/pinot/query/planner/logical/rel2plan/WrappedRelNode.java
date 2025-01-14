package org.apache.pinot.query.planner.logical.rel2plan;

import org.apache.calcite.rel.RelNode;
import org.apache.pinot.calcite.rel.PinotDataDistribution;


public class WrappedRelNode {
  private final int _nodeId;
  private final RelNode _relNode;
  private final PinotDataDistribution _pinotDataDistribution;
  private boolean _leafStageBoundary = false;

  public WrappedRelNode(int nodeId, RelNode relNode, PinotDataDistribution pinotDataDistribution) {
    _nodeId = nodeId;
    _relNode = relNode;
    _pinotDataDistribution = pinotDataDistribution;
  }

  public int getNodeId() {
    return _nodeId;
  }

  public RelNode getRelNode() {
    return _relNode;
  }

  public PinotDataDistribution getPinotDataDistribution() {
    return _pinotDataDistribution;
  }

  public boolean isLeafStageBoundary() {
    return _leafStageBoundary;
  }

  public void setLeafStageBoundary(boolean leafStageBoundary) {
    _leafStageBoundary = leafStageBoundary;
  }
}
