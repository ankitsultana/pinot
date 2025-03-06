package org.apache.pinot.query.planner.logical.rel2plan;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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
  @Nullable
  private final PinotDataDistribution _pinotDataDistribution;
  private final List<PRelNode> _inputs;
  private final boolean _leafStage;
  private final boolean _leafStageBoundary;

  public PRelNode(int nodeId, RelNode relNode, @Nullable PinotDataDistribution pinotDataDistribution) {
    this(nodeId, relNode, pinotDataDistribution, Collections.emptyList());
  }

  public PRelNode(int nodeId, RelNode relNode, @Nullable PinotDataDistribution pinotDataDistribution,
      List<PRelNode> inputs) {
    this(nodeId, relNode, pinotDataDistribution, inputs, false, false);
  }

  public PRelNode(int nodeId, RelNode relNode, @Nullable PinotDataDistribution pinotDataDistribution,
      List<PRelNode> inputs, boolean leafStage, boolean leafStageBoundary) {
    _nodeId = nodeId;
    _relNode = relNode;
    _pinotDataDistribution = pinotDataDistribution;
    _inputs = Collections.unmodifiableList(inputs);
    _leafStage = leafStage;
    _leafStageBoundary = leafStageBoundary;
  }

  public PRelNode withPinotDataDistribution(PinotDataDistribution newDistribution) {
    Preconditions.checkNotNull(newDistribution, "Attempted to set null distribution in PRelNode");
    return new PRelNode(_nodeId, _relNode, newDistribution, _inputs, _leafStage, _leafStageBoundary);
  }

  public PRelNode withNewInputs(int nodeId, List<PRelNode> newPRelInputs, PinotDataDistribution pinotDataDistribution) {
    List<RelNode> newRelInputs = new ArrayList<>();
    for (PRelNode newPRelInput : newPRelInputs) {
      newRelInputs.add(newPRelInput.getRelNode());
    }
    RelNode relNode = _relNode.copy(_relNode.getTraitSet(), newRelInputs);
    return new PRelNode(nodeId, relNode, pinotDataDistribution, newPRelInputs, _leafStage, _leafStageBoundary);
  }

  /**
   * Converts this node to a leaf stage, optionally marking it as a boundary.
   */
  public PRelNode asLeafStage(Supplier<Integer> idGenerator, boolean makeBoundary) {
    if (makeBoundary) {
      Preconditions.checkState(_inputs.isEmpty() || !_inputs.get(0).isLeafStageBoundary(),
          "Cannot make this node a boundary if its input is already a boundary");
      return new PRelNode(idGenerator.get(), _relNode, _pinotDataDistribution, _inputs, true, true);
    }
    return new PRelNode(idGenerator.get(), _relNode, _pinotDataDistribution, _inputs, true, false);
  }

  public int getNodeId() {
    return _nodeId;
  }

  public RelNode getRelNode() {
    return _relNode;
  }

  public boolean hasPinotDataDistribution() {
    return _pinotDataDistribution != null;
  }

  @Nullable
  public PinotDataDistribution getPinotDataDistribution() {
    return _pinotDataDistribution;
  }

  public PinotDataDistribution getPinotDataDistributionOrThrow() {
    Preconditions.checkNotNull(_pinotDataDistribution, "No data distribution assigned to node");
    return _pinotDataDistribution;
  }

  public boolean isLeafStage() {
    return _leafStage;
  }

  public boolean isLeafStageBoundary() {
    return _leafStageBoundary;
  }

  public List<PRelNode> getInputs() {
    return _inputs;
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
