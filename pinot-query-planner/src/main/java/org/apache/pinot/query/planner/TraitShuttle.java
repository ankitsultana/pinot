package org.apache.pinot.query.planner;

import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Objects;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalJoin;


public class TraitShuttle extends RelShuttleImpl {
  public static final TraitShuttle INSTANCE = new TraitShuttle();

  private TraitShuttle() {
  }

  @Override public RelNode visit(LogicalJoin join) {
    if (join.isSemiJoin()) {
      return join;
    }
    JoinInfo joinInfo = join.analyzeCondition();
    if (!joinInfo.isEqui()) {
      return join;
    }
    List<Integer> leftKeys = joinInfo.leftKeys;
    List<Integer> rightKeys = joinInfo.rightKeys;
    if (join.getInput(1).getTraitSet().getDistribution() != null) {
      RelDistribution rightDistribution = Objects.requireNonNull(join.getInput(1).getTraitSet().getDistribution());
      if (rightDistribution.getType() == RelDistribution.Type.BROADCAST_DISTRIBUTED) {
        return join;
      }
      throw new IllegalStateException("Unexpected distribution trait on right input of join: " + rightDistribution.getType());
    }
    Preconditions.checkState(join.getInput(0).getTraitSet().getDistribution() == null,
        "Found distribution trait on left input of join");
    join.getInput(0).getTraitSet().plus(RelDistributions.hash(leftKeys));
    join.getInput(1).getTraitSet().plus(RelDistributions.hash(rightKeys));
    return super.visit(join);
  }

  @Override public RelNode visit(LogicalAggregate aggregate) {
    Preconditions.checkState(aggregate.getInput(0).getTraitSet().getDistribution() == null, "aggregate input already has distribution trait");
    if (aggregate.getGroupCount() == 0) {
      aggregate.getInput(0).getTraitSet().plus(RelDistributions.SINGLETON);
    } else {
      aggregate.getInput(0).getTraitSet().plus(RelDistributions.hash(aggregate.getGroupSet().asList()));
    }
    return super.visit(aggregate);
  }
}
