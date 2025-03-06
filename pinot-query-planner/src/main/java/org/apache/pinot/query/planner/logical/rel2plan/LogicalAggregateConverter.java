package org.apache.pinot.query.planner.logical.rel2plan;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.pinot.calcite.rel.logical.PinotLogicalAggregate;
import org.apache.pinot.calcite.rel.logical.PinotPhysicalExchange;
import org.apache.pinot.query.planner.plannode.AggregateNode;


public class LogicalAggregateConverter {
  public PRelNode convert(PRelNode rootNode) {
    List<PRelNode> newInputs = new ArrayList<>();
    for (PRelNode input : rootNode.getInputs()) {
      newInputs.add(convert(input));
    }
    if (rootNode.getRelNode() instanceof LogicalAggregate) {
      LogicalAggregate logicalAggregate = (LogicalAggregate) rootNode.getRelNode();
      AggregateNode.AggType aggType = inferAggType(rootNode);
      PinotLogicalAggregate pinotLogicalAggregate = new PinotLogicalAggregate(logicalAggregate,
          aggType, false, RelCollations.EMPTY.getFieldCollations(), Integer.MAX_VALUE);
      PRelNode newNode = new PRelNode(rootNode.getNodeId(), pinotLogicalAggregate,
          rootNode.getPinotDataDistributionOrThrow(), newInputs);
      return newNode;
    }
    return rootNode.withNewInputs(rootNode.getNodeId(), newInputs, rootNode.getPinotDataDistributionOrThrow());
  }

  private AggregateNode.AggType inferAggType(PRelNode currentNode) {
    // TODO: Add collation and limit.
    // TODO(ankitsultana-correctness): incorrect.
    // TODO: Port logic from aggregate exchange node insert rule.
    if (currentNode.isLeafStage()) {
      return AggregateNode.AggType.LEAF;
    }
    if (currentNode.getInput(0).getRelNode() instanceof PinotPhysicalExchange) {
      if (currentNode.getInput(0).getInput(0).getRelNode() instanceof LogicalAggregate) {
        return AggregateNode.AggType.FINAL;
      }
      return AggregateNode.AggType.DIRECT;
    }
    return AggregateNode.AggType.INTERMEDIATE;
  }
}
