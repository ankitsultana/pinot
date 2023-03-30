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
import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.calcite.pinot.mappings.GeneralMapping;
import org.apache.calcite.pinot.traits.PinotRelDistribution;
import org.apache.calcite.pinot.traits.PinotRelDistributions;
import org.apache.calcite.pinot.traits.PinotTraitUtils;
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
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.hint.PinotHintStrategyTable;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.logical.LogicalWindow;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.commons.collections.MapUtils;


public class PinotRelDistributionTransformer {

  // Supported window functions
  // OTHER_FUNCTION supported are: BOOL_AND, BOOL_OR
  private static final Set<SqlKind> SUPPORTED_WINDOW_FUNCTION_KIND = ImmutableSet.of(SqlKind.SUM, SqlKind.SUM0,
      SqlKind.MIN, SqlKind.MAX, SqlKind.COUNT, SqlKind.OTHER_FUNCTION);

  private PinotRelDistributionTransformer() {
  }

  // TODO: Ensure Collation is not copied over
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
    } else if (relNode instanceof Window) {
      return applyWindow((Window) relNode, context);
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
      newTraitSet = PinotTraitUtils.setDistribution(aggregate.getInput().getTraitSet(),
          PinotRelDistributions.SINGLETON);
    } else {
      GeneralMapping mapping = GeneralMapping.infer(aggregate);
      newTraitSet = PinotTraitUtils.apply(aggregate.getInput().getTraitSet(), mapping.asTargetMapping());
    }
    return (Aggregate) aggregate.copy(newTraitSet, aggregate.getInputs());
  }

  public static Project applyProject(Project project, PinotPlannerSessionContext context) {
    GeneralMapping generalMapping = GeneralMapping.infer(project);
    Mappings.TargetMapping targetMapping = generalMapping.asTargetMapping();
    RelTraitSet relTraitSet;
    if (targetMapping == null) {
      relTraitSet = PinotTraitUtils.setDistribution(project.getInput().getTraitSet(), PinotRelDistributions.ANY);
    } else {
      relTraitSet = PinotTraitUtils.apply(project.getInput().getTraitSet(), targetMapping);
    }
    return (Project) project.copy(relTraitSet, project.getInputs());
  }

  public static Join applyJoin(Join join, PinotPlannerSessionContext context) {
    RelNode leftChild = join.getInput(0);
    RelNode rightChild = join.getInput(1);
    Set<PinotRelDistribution> leftRelDistributions = PinotTraitUtils.asSet(leftChild.getTraitSet());
    Set<PinotRelDistribution> rightRelDistributions = PinotTraitUtils.asSet(rightChild.getTraitSet());
    Optional<PinotRelDistribution> leftHashDistribution =
        leftRelDistributions.stream()
            .filter(x -> x.getType().equals(RelDistribution.Type.HASH_DISTRIBUTED)).findFirst();
    Optional<PinotRelDistribution> rightHashDistribution =
        rightRelDistributions.stream()
            .filter(x -> x.getType().equals(RelDistribution.Type.HASH_DISTRIBUTED)).findFirst();
    RelTraitSet joinTraits = RelTraitSet.createEmpty();
    GeneralMapping rightMapping = Objects.requireNonNull(GeneralMapping.infer(join).right);
    Mappings.TargetMapping rightTargetMapping = Objects.requireNonNull(rightMapping.asTargetMapping());
    if (join.getJoinType().equals(JoinRelType.INNER)) {
      JoinInfo joinInfo = join.analyzeCondition();
      Preconditions.checkState(joinInfo.isEqui(), "non-equi joins not supported yet");
      Preconditions.checkState(joinInfo.leftKeys.size() > 0 && joinInfo.rightKeys.size() > 0);
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
          joinTraits.plus(relTrait.apply(rightTargetMapping));
        }
      } else {
        joinTraits = joinTraits.plus(joinRequirement.get(1).apply(rightTargetMapping));
      }
    } else {
      throw new IllegalStateException("Only inner join supported right now");
    }
    return new LogicalJoin(join.getCluster(), joinTraits, join.getHints(), leftChild, rightChild, join.getCondition(),
        join.getVariablesSet(), join.getJoinType(), join.isSemiJoinDone(),
        ImmutableList.copyOf(join.getSystemFieldList()));
  }

  public static Window applyWindow(Window window, PinotPlannerSessionContext context) {
    // Perform all validations
    Preconditions.checkState(window instanceof LogicalWindow);
    validateWindows(window);

    LogicalWindow logicalWindow = (LogicalWindow) window;
    Window.Group windowGroup = window.groups.get(0);
    if (windowGroup.keys.isEmpty() && windowGroup.orderKeys.getKeys().isEmpty()) {
      return logicalWindow.copy(RelTraitSet.createEmpty().plus(PinotRelDistributions.SINGLETON),
          logicalWindow.getInputs());
    } else if (windowGroup.keys.isEmpty() && !windowGroup.orderKeys.getKeys().isEmpty()) {
      return logicalWindow.copy(RelTraitSet.createEmpty().plus(windowGroup.orderKeys), logicalWindow.getInputs());
    }
    boolean isPartitionByOnly = isPartitionByOnlyQuery(windowGroup);

    if (isPartitionByOnly) {
      RelTraitSet baseTraits = PinotTraitUtils.removeCollations(window.getTraitSet());
      Set<PinotRelDistribution> hashDistributions = PinotTraitUtils.asSet(baseTraits).stream()
          .filter(x -> x.getType().equals(RelDistribution.Type.HASH_DISTRIBUTED)).collect(Collectors.toSet());
      if (hashDistributions.size() > 0) {
        int numPartitions = hashDistributions.iterator().next().getNumPartitions();
        if (numPartitions != -1) {
          PinotRelDistribution desiredDistribution = PinotRelDistributions.hash(
              windowGroup.keys.asList(), numPartitions);
          for (PinotRelDistribution inputDistribution : hashDistributions) {
            if (inputDistribution.satisfies(desiredDistribution)) {
              return logicalWindow.copy(baseTraits, logicalWindow.getInputs());
            }
          }
        }
      }
      PinotRelDistribution desiredDistribution = PinotRelDistributions.hash(windowGroup.keys.asList(), -1);
      RelTraitSet traitsWithDistribution = PinotTraitUtils.setDistribution(baseTraits, desiredDistribution);
      return logicalWindow.copy(traitsWithDistribution, logicalWindow.getInputs());
    }
    // partition-by with order-by
    // TODO: It is possible to avoid shuffle here.
    RelTraitSet relTraitSet = RelTraitSet.createEmpty()
        .plus(PinotRelDistributions.hash(windowGroup.keys.asList(), -1))
        .plus(windowGroup.orderKeys);
    return logicalWindow.copy(relTraitSet, logicalWindow.getInputs());
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

  private static boolean isPartitionByOnlyQuery(Window.Group windowGroup) {
    boolean isPartitionByOnly = false;
    if (windowGroup.orderKeys.getKeys().isEmpty()) {
      return true;
    }

    if (windowGroup.orderKeys.getKeys().size() == windowGroup.keys.asList().size()) {
      Set<Integer> partitionByKeyList = new HashSet<>(windowGroup.keys.toList());
      Set<Integer> orderByKeyList = new HashSet<>(windowGroup.orderKeys.getKeys());
      isPartitionByOnly = partitionByKeyList.equals(orderByKeyList);
    }
    return isPartitionByOnly;
  }

  private static void validateWindows(Window window) {
    int numGroups = window.groups.size();
    // For Phase 1 we only handle single window groups
    Preconditions.checkState(numGroups <= 1,
        String.format("Currently only 1 window group is supported, query has %d groups", numGroups));

    // Validate that only supported window aggregation functions are present
    Window.Group windowGroup = window.groups.get(0);
    validateWindowAggCallsSupported(windowGroup);

    // Validate the frame
    validateWindowFrames(windowGroup);
  }

  private static void validateWindowAggCallsSupported(Window.Group windowGroup) {
    for (int i = 0; i < windowGroup.aggCalls.size(); i++) {
      Window.RexWinAggCall aggCall = windowGroup.aggCalls.get(i);
      SqlKind aggKind = aggCall.getKind();
      Preconditions.checkState(SUPPORTED_WINDOW_FUNCTION_KIND.contains(aggKind),
          String.format("Unsupported Window function kind: %s. Only aggregation functions are supported!", aggKind));
    }
  }

  private static void validateWindowFrames(Window.Group windowGroup) {
    // For Phase 1 only the default frame is supported
    Preconditions.checkState(!windowGroup.isRows, "Default frame must be of type RANGE and not ROWS");
    Preconditions.checkState(windowGroup.lowerBound.isPreceding() && windowGroup.lowerBound.isUnbounded(),
        String.format("Lower bound must be UNBOUNDED PRECEDING but it is: %s", windowGroup.lowerBound));
    if (windowGroup.orderKeys.getKeys().isEmpty()) {
      Preconditions.checkState(windowGroup.upperBound.isFollowing() && windowGroup.upperBound.isUnbounded(),
          String.format("Upper bound must be UNBOUNDED PRECEDING but it is: %s", windowGroup.upperBound));
    } else {
      Preconditions.checkState(windowGroup.upperBound.isCurrentRow(),
          String.format("Upper bound must be CURRENT ROW but it is: %s", windowGroup.upperBound));
    }
  }
}
