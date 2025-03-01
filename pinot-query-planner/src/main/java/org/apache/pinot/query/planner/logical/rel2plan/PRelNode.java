package org.apache.pinot.query.planner.logical.rel2plan;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.calcite.rel.RelNode;
import org.apache.pinot.calcite.rel.PinotDataDistribution;


public class PRelNode {
  private final int _nodeId;
  private final RelNode _relNode;
  private Optional<PinotDataDistribution> _pinotDataDistribution;
  private boolean _leafStage = false;
  private boolean _leafStageBoundary = false;
  private final List<PRelNode> _inputs = new ArrayList<>();

  public PRelNode(int nodeId, RelNode relNode, @Nullable PinotDataDistribution pinotDataDistribution) {
    _nodeId = nodeId;
    _relNode = relNode;
    _pinotDataDistribution = Optional.ofNullable(pinotDataDistribution);
  }

  public int getNodeId() {
    return _nodeId;
  }

  public RelNode getRelNode() {
    return _relNode;
  }

  public Optional<PinotDataDistribution> getPinotDataDistribution() {
    return _pinotDataDistribution;
  }

  public PinotDataDistribution getPinotDataDistributionOrThrow() {
    Preconditions.checkState(_pinotDataDistribution.isPresent(), "PDD missing");
    return _pinotDataDistribution.get();
  }

  public void setPinotDataDistribution(PinotDataDistribution dataDistribution) {
    Preconditions.checkState(_pinotDataDistribution.isEmpty(), "attempted to re-assign distribution to node");
    _pinotDataDistribution = Optional.of(dataDistribution);
  }

  public boolean isLeafStage() {
    return _leafStage;
  }

  public void setLeafStage(boolean leafStage) {
    _leafStage = leafStage;
  }

  public boolean isLeafStageBoundary() {
    return _leafStageBoundary;
  }

  public void setLeafStageBoundary(boolean leafStageBoundary) {
    _leafStageBoundary = leafStageBoundary;
  }

  public List<PRelNode> getInputs() {
    return _inputs;
  }

  public PRelNode getInput(int index) {
    return _inputs.get(index);
  }

  public void addInput(PRelNode input) {
    _inputs.add(input);
  }

  public PRelNode copy(int nodeId, List<PRelNode> newInputs, PinotDataDistribution pinotDataDistribution) {
    PRelNode newNode = new PRelNode(nodeId, _relNode, pinotDataDistribution);
    newInputs.forEach(newNode::addInput);
    return newNode;
  }

  public static PRelNode wrapRelTree(RelNode relNode, Supplier<Integer> nodeIdSupplier) {
    PRelNode pRelNode = new PRelNode(nodeIdSupplier.get(), relNode, null);
    for (RelNode input : relNode.getInputs()) {
      pRelNode.addInput(wrapRelTree(input, nodeIdSupplier));
    }
    return pRelNode;
  }

  public static void printWrappedRelNode(PRelNode currentNode, int level) {
    if (level > 0) {
      System.err.print("|");
    }
    for (int i = 0; i < level * 4; i++) {
      System.err.print("-");
    }
    System.err.printf("%s (nodeId=%d) %n", currentNode.getRelNode().getRelTypeName(), currentNode.getNodeId());
    for (PRelNode input : currentNode.getInputs()) {
      printWrappedRelNode(input, level + 1);
    }
  }
}
