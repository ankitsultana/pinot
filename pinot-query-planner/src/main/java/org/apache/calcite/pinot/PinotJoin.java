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

import java.util.List;
import java.util.Set;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rex.RexNode;


public class PinotJoin extends Join {
  private final boolean _semiJoinDone;

  private PinotJoin(RelOptCluster cluster, RelTraitSet traitSet, List<RelHint> hints, RelNode left, RelNode right,
      RexNode condition, Set<CorrelationId> variablesSet, JoinRelType joinRelType, boolean semiJoinDone) {
    super(cluster, traitSet, hints, left, right, condition, variablesSet, joinRelType);
    _semiJoinDone = semiJoinDone;
  }

  public boolean isSemiJoinDone() {
    return _semiJoinDone;
  }

  @Override
  public PinotJoin copy(RelTraitSet traitSet, RexNode conditionExpr, RelNode left, RelNode right, JoinRelType joinType,
      boolean semiJoinDone) {
    return new PinotJoin(getCluster(), traitSet, hints, left, right, conditionExpr, variablesSet, joinType, semiJoinDone);
  }

  public static PinotJoin of(LogicalJoin join) {
    return new PinotJoin(join.getCluster(), join.getTraitSet(), join.getHints(), join.getLeft(), join.getRight(),
        join.getCondition(), join.getVariablesSet(), join.getJoinType(), join.isSemiJoinDone());
  }
}
