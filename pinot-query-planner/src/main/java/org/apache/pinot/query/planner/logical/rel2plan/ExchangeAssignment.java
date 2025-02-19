package org.apache.pinot.query.planner.logical.rel2plan;

import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.logical.LogicalExchange;
import org.apache.pinot.calcite.rel.PinotDataDistribution;


public class ExchangeAssignment {
  private ExchangeAssignment() {
  }

  // TODO: Handle parallelism.
  // TODO: Support old features like "pre-partitioned".
  private void assign(WrappedRelNode wrappedRelNode) {
    if (wrappedRelNode.getPinotDataDistribution().isPresent()) {
      return;
    }
    // Handles distribution and collation only for now.
    if (wrappedRelNode.isLeafStage()) {
      if (wrappedRelNode.getPinotDataDistribution().isEmpty()) {
        wrappedRelNode.setPinotDataDistribution(wrappedRelNode.getInputs().get(0).getPinotDataDistribution().get());
      }
    }
    // TODO: This is wrong. keys change between inputDataDistribution, and the relDistribution of current node.
    //   We should first apply a transformation to inputDataDistribution based on the current node.
    PinotDataDistribution inputDataDistribution = wrappedRelNode.getInputs().get(0).getPinotDataDistribution().get();
    RelDistribution relDistribution = wrappedRelNode.getRelNode().getTraitSet().getDistribution();
    RelCollation relCollation = wrappedRelNode.getRelNode().getTraitSet().getCollation();
    boolean isDistributionSatisfied = inputDataDistribution.satisfies(relDistribution);
    boolean isCollationSatisfied = inputDataDistribution.satisfies(relCollation);
    if (isDistributionSatisfied && isCollationSatisfied) {
      if (wrappedRelNode.isLeafStageBoundary()) {
        // TODO: insert pass through exchange.
        LogicalExchange logicalExchange = new LogicalExchange();
      }
    } else if (!isDistributionSatisfied) {
      // TODO: Re-distribute data as required.
    } else if (!isCollationSatisfied) {
      // TODO: Sort data as required.
    } else {
      // TODO: Do both of the above.
    }
  }

  private void satisfyDistributionConstraint(WrappedRelNode currentNode, RelDistribution constraint) {
  }
}
