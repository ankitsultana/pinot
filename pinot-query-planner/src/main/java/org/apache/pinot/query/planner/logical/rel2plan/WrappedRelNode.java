package org.apache.pinot.query.planner.logical.rel2plan;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.calcite.rel.RelNode;
import org.apache.pinot.calcite.rel.PinotDataDistribution;


public class WrappedRelNode {
  public static final Supplier<Integer> DEFAULT_NODE_ID_SUPPLIER = new Supplier<Integer>() {
    private int _nodeId = 0;

    @Override
    public Integer get() {
      return _nodeId++;
    }
  };
  private final int _nodeId;
  private final RelNode _relNode;
  private Optional<PinotDataDistribution> _pinotDataDistribution;
  private boolean _leafStageBoundary = false;
  private final List<WrappedRelNode> _inputs = new ArrayList<>();

  public WrappedRelNode(int nodeId, RelNode relNode, @Nullable PinotDataDistribution pinotDataDistribution) {
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

  public void setPinotDataDistribution(PinotDataDistribution dataDistribution) {
    Preconditions.checkState(_pinotDataDistribution.isEmpty(), "attempted to re-assign distribution to node");
    _pinotDataDistribution = Optional.of(dataDistribution);
  }

  public boolean isLeafStageBoundary() {
    return _leafStageBoundary;
  }

  public void setLeafStageBoundary(boolean leafStageBoundary) {
    _leafStageBoundary = leafStageBoundary;
  }

  public List<WrappedRelNode> getInputs() {
    return _inputs;
  }

  public void addInput(WrappedRelNode input) {
    _inputs.add(input);
  }

  public WrappedRelNode copy(int nodeId, List<WrappedRelNode> newInputs, PinotDataDistribution pinotDataDistribution) {
    WrappedRelNode newNode = new WrappedRelNode(nodeId, _relNode, pinotDataDistribution);
    newInputs.forEach(newNode::addInput);
    return newNode;
  }

  public static WrappedRelNode wrapRelTree(RelNode relNode, Supplier<Integer> nodeIdSupplier) {
    WrappedRelNode wrappedRelNode = new WrappedRelNode(nodeIdSupplier.get(), relNode, null);
    for (RelNode input : relNode.getInputs()) {
      wrappedRelNode.addInput(wrapRelTree(input, nodeIdSupplier));
    }
    return wrappedRelNode;
  }
}
