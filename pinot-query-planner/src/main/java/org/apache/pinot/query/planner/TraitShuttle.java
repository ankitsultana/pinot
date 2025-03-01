package org.apache.pinot.query.planner;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalWindow;
import org.apache.calcite.util.ImmutableIntList;


public class TraitShuttle extends RelShuttleImpl {
  public static final TraitShuttle INSTANCE = new TraitShuttle();

  private TraitShuttle() {
  }

  @Override
  public RelNode visit(LogicalSort sort) {
    sort = (LogicalSort) super.visit(sort);
    // Current behavior is to always converge to a single server for Sort, so add the SINGLETON trait.
    return sort.copy(sort.getTraitSet().plus(RelDistributions.SINGLETON).plus(sort.collation), sort.getInputs());
  }

  @Override
  public RelNode visit(RelNode other) {
    if (other instanceof LogicalWindow) {
      return visitWindow((LogicalWindow) other);
    }
    return super.visit(other);
  }

  @Override public RelNode visit(LogicalJoin join) {
    join = (LogicalJoin) super.visit(join);
    List<RelNode> newInputs = join.getInputs();
    RelNode leftInput = newInputs.get(0);
    RelNode rightInput = newInputs.get(1);
    if (join.isSemiJoin()) {
      Preconditions.checkState(rightInput.getTraitSet().getDistribution() == null, "Found existing dist trait on right input of semi-join");
      rightInput = rightInput.copy(rightInput.getTraitSet().plus(RelDistributions.BROADCAST_DISTRIBUTED), rightInput.getInputs());
      return join.copy(join.getTraitSet(), ImmutableList.of(leftInput, rightInput));
    }
    JoinInfo joinInfo = join.analyzeCondition();
    Preconditions.checkState(joinInfo.isEqui(), "non-equi joins are not supported yet");
    if (!joinInfo.isEqui()) {
      return join.copy(join.getTraitSet(), newInputs);
    }
    List<Integer> leftKeys = joinInfo.leftKeys;
    List<Integer> rightKeys = joinInfo.rightKeys;
    if (rightInput.getTraitSet().getDistribution() != null) {
      RelDistribution rightDistribution = Objects.requireNonNull(rightInput.getTraitSet().getDistribution());
      if (rightDistribution.getType() == RelDistribution.Type.BROADCAST_DISTRIBUTED) {
        return join.copy(join.getTraitSet(), ImmutableList.of(leftInput, rightInput));
      }
      throw new IllegalStateException("Unexpected distribution trait on right input of join: " + rightDistribution.getType());
    }
    Preconditions.checkState(leftInput.getTraitSet().getDistribution() == null,
        "Found distribution trait on left input of join");
    leftInput = leftInput.copy(leftInput.getTraitSet().plus(RelDistributions.hash(leftKeys)), leftInput.getInputs());
    rightInput = rightInput.copy(rightInput.getTraitSet().plus(RelDistributions.hash(rightKeys)), rightInput.getInputs());
    return join.copy(join.getTraitSet(), ImmutableList.of(leftInput, rightInput));
  }

  @Override public RelNode visit(LogicalAggregate aggregate) {
    Preconditions.checkState(aggregate.getInput(0).getTraitSet().getDistribution() == null, "aggregate input already has distribution trait");
    aggregate = (LogicalAggregate) super.visit(aggregate);
    RelNode newInput = aggregate.getInput(0);
    if (aggregate.getGroupCount() == 0) {
      newInput = newInput.copy(newInput.getTraitSet().plus(RelDistributions.SINGLETON), newInput.getInputs());
    } else {
      newInput = newInput.copy(newInput.getTraitSet().plus(RelDistributions.hash(aggregate.getGroupSet().asList())), newInput.getInputs());
    }
    return aggregate.copy(aggregate.getTraitSet(), ImmutableList.of(newInput));
  }

  private RelNode visitWindow(LogicalWindow window) {
    Preconditions.checkState(window.groups.size() <= 1, "Different partition-by clause not allowed in window function yet");
    window = (LogicalWindow) super.visit(window);
    RelCollation windowGroupCollation = getCollation(window);
    RelNode newInput = window.getInput(0);
    if (window.groups.isEmpty() || window.groups.get(0).keys.isEmpty()) {
      RelTraitSet newTraitSet = newInput.getTraitSet().plus(RelDistributions.SINGLETON);
      if (!windowGroupCollation.getKeys().isEmpty()) {
        if (newInput instanceof Sort) {
          Sort sortInput = (Sort) newInput;
          if (!sortInput.getCollation().equals(windowGroupCollation)) {
            newInput = LogicalSort.create(sortInput, windowGroupCollation,  null, null);
            newTraitSet = newInput.getTraitSet().plus(RelDistributions.SINGLETON);
          }
        } else {
          newTraitSet = newTraitSet.plus(windowGroupCollation);
        }
      }
      newInput = newInput.copy(newTraitSet, newInput.getInputs());
    } else {
      Window.Group group = window.groups.get(0);
      List<Integer> partitionKeys = group.keys.asList();
      if (newInput instanceof LogicalSort) {
        LogicalSort inputSort = (LogicalSort) newInput;
        LogicalSort newSort = LogicalSort.create(inputSort, windowGroupCollation,  null, null);
        newSort = (LogicalSort) newSort.copy(newSort.getTraitSet().plus(RelDistributions.hash(partitionKeys)).plus(
                newSort.collation), newSort.getInputs());
        newInput = newSort;
      } else {
        RelTraitSet newTraitSet = newInput.getTraitSet().plus(RelDistributions.hash(partitionKeys));
        if (!windowGroupCollation.getKeys().isEmpty()) {
          newTraitSet = newTraitSet.plus(windowGroupCollation);
        }
        newInput = newInput.copy(newTraitSet, newInput.getInputs());
      }
    }
    return window.copy(window.getTraitSet(), ImmutableList.of(newInput));
  }

  private RelCollation getCollation(LogicalWindow window) {
    return window.groups.isEmpty() ? RelCollations.EMPTY : window.groups.get(0).collation();
  }

  private List<Integer> getOrderKeys(LogicalWindow window) {
    return window.groups.isEmpty() ? Collections.emptyList() : window.groups.get(0).orderKeys.getKeys();
  }
}
