package org.apache.pinot.query.planner.logical.rel2plan;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.pinot.calcite.rel.logical.PinotLogicalAggregate;
import org.apache.pinot.query.planner.plannode.AggregateNode;


public class LogicalAggregateConverter {
  public PRelNode convert(PRelNode rootNode) {
    List<PRelNode> newInputs = new ArrayList<>();
    for (PRelNode input : newInputs) {
      newInputs.add(convert(input));
    }
    if (rootNode.getRelNode() instanceof LogicalAggregate) {
      LogicalAggregate logicalAggregate = (LogicalAggregate) rootNode.getRelNode();
      // TODO(ankitsultana-correctness): this is incorrect.
      PinotLogicalAggregate pinotLogicalAggregate = new PinotLogicalAggregate(logicalAggregate,
          AggregateNode.AggType.DIRECT, false, RelCollations.EMPTY, Integer.MAX_VALUE);
      return new PRelNode(rootNode.getNodeId(), pinotLogicalAggregate, rootNode.getPinotDataDistributionOrThrow());
    }
    return rootNode.copy(rootNode.getNodeId(), newInputs, rootNode.getPinotDataDistributionOrThrow());
  }
}
