package org.apache.pinot.query.planner.logical.rel2plan;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.calcite.rel.RelNode;
import org.apache.pinot.calcite.rel.PinotDataDistribution;


/**
 * Wrapper around Calcite RelNodes to allow tracking metadata without having to deal with RelMetadataQuery and the
 * like. The tree formed by PRelNode and RelNode should always be the same.
 */
public class PRelNode {
  private final int _nodeId;
  private final RelNode _relNode;
  private final Optional<PinotDataDistribution> _pinotDataDistribution;
  private final List<PRelNode> _inputs = new ArrayList<>();
  private boolean _leafStage = false;
  private boolean _leafStageBoundary = false;

  public PRelNode(int nodeId, RelNode relNode, @Nullable PinotDataDistribution pinotDataDistribution) {
    _nodeId = nodeId;
    _relNode = relNode;
    _pinotDataDistribution = Optional.ofNullable(pinotDataDistribution);
  }

  public PRelNode(int nodeId, RelNode relNode, @Nullable PinotDataDistribution pinotDataDistribution,
      List<PRelNode> inputs) {
    _nodeId = nodeId;
    _relNode = relNode;
    _pinotDataDistribution = Optional.ofNullable(pinotDataDistribution);
    _inputs.addAll(inputs);
  }

  public PRelNode withPinotDataDistribution(PinotDataDistribution newDistribution) {
    return new PRelNode(_nodeId, _relNode, newDistribution);
  }

  public PRelNode copy(int nodeId, List<PRelNode> newPRelInputs, PinotDataDistribution pinotDataDistribution) {
    List<RelNode> newRelInputs = new ArrayList<>();
    for (PRelNode newPRelInput : newPRelInputs) {
      newRelInputs.add(newPRelInput.getRelNode());
    }
    RelNode relNode = _relNode.copy(_relNode.getTraitSet(), newRelInputs);
    return new PRelNode(nodeId, relNode, pinotDataDistribution, newPRelInputs);
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
    return Collections.unmodifiableList(_inputs);
  }

  public PRelNode getInput(int index) {
    return _inputs.get(index);
  }

  public static PRelNode wrapRelTree(RelNode relNode, Supplier<Integer> nodeIdSupplier) {
    List<PRelNode> newInputs = new ArrayList<>();
    for (RelNode input : relNode.getInputs()) {
      newInputs.add(wrapRelTree(input, nodeIdSupplier));
    }
    return new PRelNode(nodeIdSupplier.get(), relNode, null, newInputs);
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
