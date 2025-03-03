/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.calcite.rel.rules;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.tools.RelBuilderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PinotTraitConstraintRule extends RelOptRule {
  public static final PinotTraitConstraintRule INSTANCE = new PinotTraitConstraintRule(
      PinotRuleUtils.PINOT_REL_FACTORY);
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotTraitConstraintRule.class);

  public PinotTraitConstraintRule(RelBuilderFactory factory) {
    super(operand(RelNode.class, any()), factory, null);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    RelNode current = call.rel(0);
    if (current instanceof Join) {
      call.transformTo(attachTraitToJoin((LogicalJoin) current));
    } else if (current instanceof Aggregate) {
      call.transformTo(attachTraitToAggregate((LogicalAggregate) current));
    }
  }

  private static RelNode attachTraitToJoin(LogicalJoin join) {
    // TODO: Need to handle more cases.
    JoinInfo joinInfo = join.analyzeCondition();
    if (join.isSemiJoin()) {
      LOGGER.warn("We don't assign any trait to semi-join, and instead assume its trait directly");
      return join;
    }
    if (!joinInfo.isEqui()) {
      LOGGER.warn("Non-equi join found.. unhandled condition");
      return join;
    }
    RelTraitSet currentTraitSet = join.getCluster().traitSet();
    List<Integer> leftTrait = new ArrayList<>(joinInfo.leftKeys);
    List<Integer> rightTrait = new ArrayList<>(joinInfo.rightKeys);
    int leftInputFieldCount = join.getInput(0).getRowType().getFieldCount();
    rightTrait = rightTrait.stream().map(x -> {
      return x + leftInputFieldCount;
    }).collect(Collectors.toList());
    currentTraitSet = addTraits(currentTraitSet, List.of(RelDistributions.hash(leftTrait),
        RelDistributions.hash(rightTrait)));
    return new LogicalJoin(join.getCluster(), currentTraitSet, join.getLeft(),
        join.getRight(), join.getCondition(), join.getVariablesSet(), join.getJoinType(), join.isSemiJoinDone(),
        ImmutableList.copyOf(join.getSystemFieldList()));
  }

  private static RelNode attachTraitToAggregate(LogicalAggregate aggregate) {
    // TODO: Need to handle more cases.
    List<Integer> groupSet = aggregate.getGroupSet().asList();
    RelTraitSet currentTraitSet = aggregate.getCluster().traitSet();
    currentTraitSet = addTraits(currentTraitSet, List.of(RelDistributions.hash(groupSet)));
    return new LogicalAggregate(aggregate.getCluster(), currentTraitSet, aggregate.getHints(),
        aggregate.getInput(), aggregate.getGroupSet(), aggregate.getGroupSets(), aggregate.getAggCallList());
  }

  private static RelTraitSet addTraits(RelTraitSet currentTraitSet, List<RelDistribution> newTraits) {
    if (currentTraitSet.getTraits(RelDistributionTraitDef.INSTANCE) != null
        && !currentTraitSet.getTraits(RelDistributionTraitDef.INSTANCE).isEmpty()) {
      List<RelDistribution> traitsToAdd = new ArrayList<>(currentTraitSet.getTraits(RelDistributionTraitDef.INSTANCE));
      traitsToAdd.addAll(newTraits);
      return currentTraitSet.replace(traitsToAdd);
    }
    currentTraitSet = currentTraitSet.plus(newTraits.get(0));
    if (newTraits.size() == 1) {
      return currentTraitSet;
    }
    return currentTraitSet.replace(newTraits);
  }
}
