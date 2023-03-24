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
package org.apache.calcite.rel.rules;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.Map;
import org.apache.calcite.pinot.PinotPlannerSessionContext;
import org.apache.calcite.pinot.PinotRelParallelismTrait;
import org.apache.calcite.pinot.PinotRelParallelismTraitDef;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.TableConfig;


public class PinotParallelismRule extends RelOptRule {
  public static final PinotParallelismRule INSTANCE = new PinotParallelismRule(PinotRuleUtils.PINOT_REL_FACTORY);

  protected PinotParallelismRule(RelBuilderFactory factory) {
    super(operand(RelNode.class, any()), factory, null);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    if (call.rels.length < 1) {
      return false;
    }
    Preconditions.checkState(call.rels.length == 1);
    return call.rel(0).getTraitSet().getTrait(PinotRelParallelismTraitDef.INSTANCE) == null;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    PinotPlannerSessionContext context = (PinotPlannerSessionContext) call.getPlanner().getContext();
    int queryParallelism = context.getQueryParallelism();
    RelNode relNode = call.rel(0);
    Preconditions.checkState(!(relNode instanceof HepRelVertex));
    RelNode newRelNode = null;
    if (relNode instanceof TableScan) {
      LogicalTableScan tableScan = (LogicalTableScan) relNode;
      int parallelism = queryParallelism;
      if (parallelism == -1) {
        TableConfig tableConfig = context.getTableConfig(tableScan.getTable().getQualifiedName().get(0));
        Preconditions.checkNotNull(tableConfig, "");
        Map<String, ColumnPartitionConfig> columnPartitionConfigMap =
            PinotPlannerSessionContext.getTablePartitionConfigSafe(tableConfig);
        if (columnPartitionConfigMap != null) {
          parallelism = columnPartitionConfigMap.values().iterator().next().getNumPartitions();
        }
      }
      newRelNode = new LogicalTableScan(relNode.getCluster(),
          relNode.getTraitSet().plus(new PinotRelParallelismTrait(parallelism)), tableScan.getHints(),
          tableScan.getTable());
    } else if (relNode instanceof LogicalJoin) {
      LogicalJoin join = (LogicalJoin) relNode;
      int parallelism = queryParallelism;
      if (parallelism == -1) {
        RelNode left = join.getLeft();
        RelNode right = join.getRight();
        PinotRelParallelismTrait leftParallelism = left.getTraitSet().getTrait(PinotRelParallelismTraitDef.INSTANCE);
        PinotRelParallelismTrait rightParallelism = right.getTraitSet().getTrait(PinotRelParallelismTraitDef.INSTANCE);
        Preconditions.checkState(leftParallelism != null && rightParallelism != null);
        parallelism = Math.max(leftParallelism.getParallelism(), rightParallelism.getParallelism());
      }
      newRelNode = new LogicalJoin(join.getCluster(),
          join.getTraitSet().plus(new PinotRelParallelismTrait(parallelism)), join.getHints(), join.getLeft(),
          join.getRight(), join.getCondition(), join.getVariablesSet(), join.getJoinType(), join.isSemiJoinDone(),
          ImmutableList.copyOf(join.getSystemFieldList()));
    } else {
      Preconditions.checkState(relNode instanceof SingleRel);
      SingleRel singleRel = (SingleRel) relNode;
      int parallelism = queryParallelism;
      if (parallelism == -1) {
        PinotRelParallelismTrait trait =
            singleRel.getInput().getTraitSet().getTrait(PinotRelParallelismTraitDef.INSTANCE);
        if (trait != null) {
          parallelism = trait.getParallelism();
        }
      }
      RelTraitSet relTraitSet = relNode.getTraitSet().plus(new PinotRelParallelismTrait(parallelism));
      newRelNode = relNode.copy(relTraitSet, relNode.getInputs());
    }
    Preconditions.checkNotNull(newRelNode.getTraitSet().getTrait(PinotRelParallelismTraitDef.INSTANCE),
        String.format("Trait not copied for: %s", newRelNode.getClass()));
    call.transformTo(newRelNode);
  }
}
