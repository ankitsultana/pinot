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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.core.SortExchange;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.PinotHintStrategyTable;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalExchange;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.commons.collections.MapUtils;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;


public class PinotRelIdentityHashOptShuttle extends RelShuttleImpl {
  private final TableCache _tableCache;
  private final Map<String, String> _options;

  public PinotRelIdentityHashOptShuttle(TableCache tableCache, Map<String, String> options) {
    _tableCache = tableCache;
    _options = options;
  }

  @Override
  public RelNode visit(TableScan tableScan) {
    List<RelHint> partitioningHints = tableScan.getHints().stream()
        .filter(hint -> hint.hintName.equals(PinotHintStrategyTable.TABLE_PARTITION_HINT_KEY))
        .collect(Collectors.toList());
    if (!partitioningHints.isEmpty()) {
      Map<String, String> kvOptions = partitioningHints.get(0).kvOptions;
      if (MapUtils.isNotEmpty(kvOptions) && kvOptions.containsKey("column")) {
        String columnName = kvOptions.get("column");
        PinotRelDistribution relDistribution = PinotRelDistribution.create(columnName, tableScan.getRowType());
        return new LogicalTableScan(tableScan.getCluster(), tableScan.getTraitSet().plus(relDistribution),
            tableScan.getHints(), tableScan.getTable());
      }
      return new LogicalTableScan(tableScan.getCluster(), tableScan.getTraitSet().plus(PinotRelDistributions.ANY),
          tableScan.getHints(), tableScan.getTable());
    }
    PinotRelDistribution pinotRelDistribution =
        PinotRelDistribution.create(
            getTableConfig(tableScan.getTable().getQualifiedName().get(0)), tableScan.getRowType());
    return new LogicalTableScan(tableScan.getCluster(), tableScan.getTraitSet().plus(pinotRelDistribution),
        tableScan.getHints(), tableScan.getTable());
  }

  @Override
  public RelNode visit(LogicalJoin join) {
    join = (LogicalJoin) super.visitChildren(join);
    if (isBroadcastJoin(join)) {
    }
    RelNode leftChild = join.getInput(0);
    RelNode rightChild = join.getInput(1);
    int leftFieldCount = leftChild.getRowType().getFieldNames().size();
    RelTraitSet rightOffsetTraits = RelTraitSet.createEmpty();
    for (RelTrait relTrait : rightChild.getTraitSet()) {
      if (relTrait.getTraitDef().equals(PinotRelDistributionTraitDef.INSTANCE)) {
        PinotRelDistribution pinotRelDistribution = (PinotRelDistribution) relTrait;
        if (pinotRelDistribution.getType().equals(RelDistribution.Type.ANY)) {
          continue;
        }
        if (pinotRelDistribution.getType().equals(RelDistribution.Type.HASH_DISTRIBUTED)) {
          List<Integer> newKeys = pinotRelDistribution.getKeys().stream()
              .map(x -> x + leftFieldCount)
              .collect(Collectors.toList());
          rightOffsetTraits.plus(PinotRelDistributions.hash(newKeys, pinotRelDistribution.getNumPartitions()));
        } else {
          rightOffsetTraits.plus(relTrait);
        }
      } else {
        rightOffsetTraits.plus(relTrait);
      }
    }
    RelTraitSet allTraits = leftChild.getTraitSet().plusAll(rightOffsetTraits.toArray(new RelTrait[0]));
    return new LogicalJoin(join.getCluster(), allTraits, join.getHints(), leftChild, rightChild, join.getCondition(),
        join.getVariablesSet(), join.getJoinType(), join.isSemiJoinDone(),
        ImmutableList.copyOf(join.getSystemFieldList()));
  }

  @Override
  public RelNode visit(LogicalAggregate aggregate) {
    aggregate = (LogicalAggregate) super.visitChildren(aggregate);
    Map<Integer, Integer> oldToNewIndex = new HashMap<>();
    List<Integer> groupSet = aggregate.getGroupSet().asList();
    for (int i = 0; i < groupSet.size(); i++) {
      oldToNewIndex.put(groupSet.get(i), i);
    }
    RelTraitSet relTraitSet = apply(aggregate.getInput().getTraitSet(), MapBasedTargetMapping.of(oldToNewIndex));
    return aggregate.copy(relTraitSet, aggregate.getInputs());
  }

  @Override
  public RelNode visit(LogicalProject project) {
    project = (LogicalProject) super.visitChildren(project);
    Map<Integer, Integer> oldToNewIndex = new HashMap<>();
    for (int i = 0; i < project.getProjects().size(); i++) {
      RexNode p = project.getProjects().get(i);
      if (p instanceof RexInputRef) {
        // TODO: This won't work for multi-mapping
        oldToNewIndex.put(((RexInputRef) p).getIndex(), i);
      }
    }
    RelTraitSet relTraitSet = apply(project.getInput().getTraitSet(), MapBasedTargetMapping.of(oldToNewIndex));
    return project.copy(relTraitSet, project.getInputs());
  }

  @Override
  public RelNode visit(LogicalValues values) {
    values = (LogicalValues) super.visitChildren(values);
    return values.copy(RelTraitSet.createEmpty().plus(PinotRelDistributions.ANY), values.getInputs());
  }

  @Override
  public RelNode visit(LogicalFilter filter) {
    filter = (LogicalFilter) super.visitChildren(filter);
    return filter.copy(filter.getInput().getTraitSet(), filter.getInputs());
  }

  @Override
  public RelNode visit(LogicalSort sort) {
    sort = (LogicalSort) super.visitChildren(sort);
    return sort.copy(sort.getInput().getTraitSet(), sort.getInputs());
  }

  @Override
  public RelNode visit(LogicalExchange exchange) {
    throw new UnsupportedOperationException("");
  }

  @Override
  public RelNode visit(RelNode other) {
    if (other instanceof PinotExchange) {
      return this.visit((PinotExchange) other);
    } else if (other instanceof PinotSortExchange) {
      return this.visit((PinotSortExchange) other);
    } else if (other instanceof SortExchange) {
      return this.visit((SortExchange) other);
    }
    throw new IllegalStateException(String.format("Found instance: %s", other.getClass()));
  }

  public RelNode visit(PinotExchange pinotExchange) {
    pinotExchange = (PinotExchange) super.visitChildren(pinotExchange);
    RelNode child = pinotExchange.getInput();

    List<PinotRelDistribution> exchangeDistributions =
        pinotExchange.getTraitSet().getTraits(PinotRelDistributionTraitDef.INSTANCE);
    Preconditions.checkState((Objects.requireNonNull(exchangeDistributions)).size() == 1);
    PinotRelDistribution exchangeDistribution = exchangeDistributions.get(0);
    List<PinotRelDistribution> inputDistributions =
        child.getTraitSet().getTraits(PinotRelDistributionTraitDef.INSTANCE);
    Preconditions.checkNotNull(inputDistributions);
    boolean satisfies = false;
    for (PinotRelDistribution inputDistribution : inputDistributions) {
      if (inputDistribution.satisfies(exchangeDistribution)) {
        satisfies = true;
        break;
      }
    }
    return satisfies ? PinotExchange.createIdentity(child) : pinotExchange;
  }

  public RelNode visit(PinotSortExchange pinotSortExchange) {
    return pinotSortExchange;
  }

  public RelNode visit(SortExchange sortExchange) {
    sortExchange = (SortExchange) super.visitChildren(sortExchange);
    return PinotSortExchange.create(sortExchange.getInput(),
        PinotRelDistribution.of(sortExchange.getDistribution()), sortExchange.getCollation());
  }

  private RelTraitSet apply(RelTraitSet inputTraits, Mappings.TargetMapping mapping) {
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

  private TableConfig getTableConfig(String tableName) {
    String rawTableName = TableNameBuilder.extractRawTableName(tableName);
    TableConfig tableConfig = _tableCache.getTableConfig(TableNameBuilder.OFFLINE.tableNameWithType(rawTableName));
    if (tableConfig != null) {
      return tableConfig;
    }
    return _tableCache.getTableConfig(TableNameBuilder.REALTIME.tableNameWithType(rawTableName));
  }

  private boolean isBroadcastJoin(LogicalJoin join) {
    RelNode right = join.getRight();
    if (right instanceof Exchange) {
      Exchange exchange = (Exchange) right;
      return exchange.getDistribution().getType().equals(RelDistribution.Type.BROADCAST_DISTRIBUTED);
    }
    return false;
  }
}
