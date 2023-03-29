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
package org.apache.calcite.pinot;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.hint.PinotHintStrategyTable;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.commons.collections.MapUtils;


public class PinotRelDistributionTransformer {

  public PinotRelDistributionTransformer() {
  }

  public static RelNode dispatch(RelNode relNode, PinotPlannerSessionContext context) {
    if (relNode instanceof TableScan) {
      return applyTableScan((TableScan) relNode, context);
    } else if (relNode instanceof Aggregate) {
      return applyAggregate((Aggregate) relNode, context);
    } else if (relNode instanceof Project) {
      return applyProject((Project) relNode, context);
    } else if (relNode instanceof Join) {
      return applyJoin((Join) relNode, context);
    } else if (relNode instanceof Values) {
      return applyValues((Values) relNode, context);
    } else if (relNode instanceof Filter) {
      return applyFilter((Filter) relNode, context);
    } else if (relNode instanceof Sort) {
      return applySort((Sort) relNode, context);
    } else {
      throw new UnsupportedOperationException(String.format("Found: %s", relNode.getClass()));
    }
  }

  public static TableScan applyTableScan(TableScan tableScan, PinotPlannerSessionContext context) {
    List<RelHint> partitioningHints = tableScan.getHints().stream()
        .filter(hint -> hint.hintName.equals(PinotHintStrategyTable.TABLE_PARTITION_HINT_KEY))
        .collect(Collectors.toList());
    if (!partitioningHints.isEmpty()) {
      Map<String, String> kvOptions = partitioningHints.get(0).kvOptions;
      if (MapUtils.isNotEmpty(kvOptions) && kvOptions.containsKey("column")) {
        String columnName = kvOptions.get("column");
        Integer numPartitions = Integer.parseInt(kvOptions.get("partitions"));
        PinotRelDistribution relDistribution = PinotRelDistributions.hash(
            columnName, numPartitions, tableScan.getRowType());
        return new LogicalTableScan(tableScan.getCluster(), tableScan.getTraitSet().plus(relDistribution),
            tableScan.getHints(), tableScan.getTable());
      }
      return new LogicalTableScan(tableScan.getCluster(), tableScan.getTraitSet().plus(PinotRelDistributions.ANY),
          tableScan.getHints(), tableScan.getTable());
    }
    PinotRelDistribution pinotRelDistribution =
        PinotRelDistributions.of(
            context.getTableConfig(tableScan.getTable().getQualifiedName().get(0)), tableScan.getRowType());
    return new LogicalTableScan(tableScan.getCluster(), tableScan.getTraitSet().plus(pinotRelDistribution),
        tableScan.getHints(), tableScan.getTable());
  }

  public static Aggregate applyAggregate(Aggregate aggregate, PinotPlannerSessionContext context) {
    Preconditions.checkState(aggregate.getGroupSets().size() <= 1, "Grouping sets are not supported at the moment");
    RelTraitSet newTraitSet;
    if (aggregate.getGroupCount() == 0) {
      newTraitSet = make(aggregate.getInput().getTraitSet(), PinotRelDistributions.SINGLETON);
    } else {
      Map<Integer, Integer> oldToNewIndex = new HashMap<>();
      List<Integer> groupSet = aggregate.getGroupSet().asList();
      for (int i = 0; i < groupSet.size(); i++) {
        oldToNewIndex.put(groupSet.get(i), i);
      }
      newTraitSet = applyMapping(aggregate.getInput().getTraitSet(), MapBasedTargetMapping.of(oldToNewIndex));
    }
    return (Aggregate) aggregate.copy(newTraitSet, aggregate.getInputs());
  }

  public static Project applyProject(Project project, PinotPlannerSessionContext context) {
    Map<Integer, Integer> oldToNewIndex = new HashMap<>();
    boolean isComplex = false;
    for (int i = 0; i < project.getProjects().size(); i++) {
      RexNode p = project.getProjects().get(i);
      int oldIndex = -1;
      if (p instanceof RexInputRef) {
        oldIndex = ((RexInputRef) p).getIndex();
      } else {
        List<RexInputRef> rexNodes = exploreRexInputRef(p);
        if (rexNodes.size() == 1) {
          oldIndex = rexNodes.get(0).getIndex();
        } else if (rexNodes.size() > 1) {
          isComplex = true;
          break;
        }
      }
      if (oldToNewIndex.containsKey(oldIndex)) {
        isComplex = true;
        break;
      }
      oldToNewIndex.put(oldIndex, i);
    }
    RelTraitSet relTraitSet;
    if (isComplex) {
      // TODO: Happens when either we have two columns in a RexCall or if duplicate columns. In this case we drop all
      // DistributionTraits.
      relTraitSet = make(project.getInput().getTraitSet(), PinotRelDistributions.ANY);
    } else {
      relTraitSet = applyMapping(project.getInput().getTraitSet(), MapBasedTargetMapping.of(oldToNewIndex));
    }
    return (Project) project.copy(relTraitSet, project.getInputs());
  }

  public static Join applyJoin(Join join, PinotPlannerSessionContext context) {
    RelNode leftChild = join.getInput(0);
    RelNode rightChild = join.getInput(1);
    Set<PinotRelDistribution> leftRelDistributions = filterPinotRelDistribution(leftChild.getTraitSet());
    Set<PinotRelDistribution> rightRelDistributions = filterPinotRelDistribution(rightChild.getTraitSet());
    Optional<PinotRelDistribution> leftHashDistribution =
        leftRelDistributions.stream()
            .filter(x -> x.getType().equals(RelDistribution.Type.HASH_DISTRIBUTED)).findFirst();
    Optional<PinotRelDistribution> rightHashDistribution =
        rightRelDistributions.stream()
            .filter(x -> x.getType().equals(RelDistribution.Type.HASH_DISTRIBUTED)).findFirst();
    RelTraitSet joinTraits = RelTraitSet.createEmpty();
    int leftFieldCount = leftChild.getRowType().getFieldNames().size();
    OffsetTargetMapping offsetTargetMapping =
        new OffsetTargetMapping(rightChild.getRowType().getFieldCount(), leftFieldCount);
    if (join.getJoinType().equals(JoinRelType.INNER)) {
      JoinInfo joinInfo = join.analyzeCondition();
      Preconditions.checkState(joinInfo.isEqui(), "non-equi joins not supported yet");
      int numPartitions = -1;
      if (leftHashDistribution.isPresent()) {
        numPartitions = leftHashDistribution.get().getNumPartitions();
      }
      if (rightHashDistribution.isPresent()) {
        numPartitions = Math.max(numPartitions, rightHashDistribution.get().getNumPartitions());
      }
      List<PinotRelDistribution> joinRequirement = new ArrayList<>();
      joinRequirement.add(PinotRelDistributions.hash(joinInfo.leftKeys, numPartitions));
      joinRequirement.add(PinotRelDistributions.hash(joinInfo.rightKeys, numPartitions));
      boolean leftSatisfied = leftRelDistributions.stream().anyMatch(x -> x.satisfies(joinRequirement.get(0)));
      boolean rightSatisfied = rightRelDistributions.stream().anyMatch(x -> x.satisfies(joinRequirement.get(1)));
      if (leftSatisfied) {
        joinTraits = joinTraits.plusAll(leftChild.getTraitSet().toArray(new RelTrait[0]));
      } else {
        joinTraits = joinTraits.plus(joinRequirement.get(0));
      }
      if (rightSatisfied) {
        for (RelTrait relTrait : rightChild.getTraitSet()) {
          joinTraits.plus(relTrait.apply(offsetTargetMapping));
        }
      } else {
        joinTraits = joinTraits.plus(joinRequirement.get(1).apply(offsetTargetMapping));
      }
    } else {
      throw new IllegalStateException("Only inner join supported right now");
    }
    return new LogicalJoin(join.getCluster(), joinTraits, join.getHints(), leftChild, rightChild, join.getCondition(),
        join.getVariablesSet(), join.getJoinType(), join.isSemiJoinDone(),
        ImmutableList.copyOf(join.getSystemFieldList()));
  }

  public static Values applyValues(Values values, PinotPlannerSessionContext context) {
    return (Values) values.copy(RelTraitSet.createEmpty().plus(PinotRelDistributions.ANY), values.getInputs());
  }

  public static Filter applyFilter(Filter filter, PinotPlannerSessionContext context) {
    return (Filter) filter.copy(filter.getInput().getTraitSet(), filter.getInputs());
  }

  public static Sort applySort(Sort sort, PinotPlannerSessionContext context) {
    RelTraitSet newTraitSet = sort.getTraitSet().plus(sort.getCollation());
    return sort.copy(newTraitSet, sort.getInputs());
  }

  private static Set<PinotRelDistribution> filterPinotRelDistribution(RelTraitSet relTraitSet) {
    Set<PinotRelDistribution> result = new HashSet<>();
    for (RelTrait relTrait : relTraitSet) {
      if (relTrait.getTraitDef().equals(PinotRelDistributionTraitDef.INSTANCE)) {
        result.add((PinotRelDistribution) relTrait);
      }
    }
    return result;
  }

  private static RelTraitSet make(RelTraitSet inputTraits, PinotRelDistribution distribution) {
    RelTraitSet result = RelTraitSet.createEmpty();
    for (RelTrait trait : inputTraits) {
      if (!trait.getTraitDef().equals(PinotRelDistributionTraitDef.INSTANCE)) {
        result = result.plus(trait);
      }
    }
    return result.plus(distribution);
  }

  private static RelTraitSet applyMapping(RelTraitSet inputTraits, @Nullable Mappings.TargetMapping mapping) {
    RelTraitSet relTraitSet = RelTraitSet.createEmpty();
    boolean added = false;
    for (RelTrait trait : inputTraits) {
      if (!trait.getTraitDef().equals(PinotRelDistributionTraitDef.INSTANCE)) {
        relTraitSet = relTraitSet.plus(trait);
      } else {
        PinotRelDistribution pinotRelDistribution = (PinotRelDistribution) trait;
        PinotRelDistribution newDistribution = (PinotRelDistribution) pinotRelDistribution.apply(mapping);
        if (!newDistribution.getType().equals(RelDistribution.Type.ANY)) {
          added = true;
          relTraitSet = relTraitSet.plus(newDistribution);
        }
      }
    }
    if (!added) {
      relTraitSet = relTraitSet.plus(PinotRelDistributions.ANY);
    }
    return relTraitSet;
  }

  private static List<RexInputRef> exploreRexInputRef(RexNode rexNode) {
    if (rexNode instanceof RexCall) {
      List<RexInputRef> result = new ArrayList<>();
      RexCall rexCall = (RexCall) rexNode;
      for (RexNode operand : rexCall.getOperands()) {
        result.addAll(exploreRexInputRef(operand));
      }
      return result;
    } else if (rexNode instanceof RexInputRef) {
      return Collections.singletonList((RexInputRef) rexNode);
    } else if (rexNode instanceof RexLiteral) {
      return Collections.emptyList();
    }
    throw new UnsupportedOperationException(String.format("Found: %s", rexNode.getClass()));
  }
}
