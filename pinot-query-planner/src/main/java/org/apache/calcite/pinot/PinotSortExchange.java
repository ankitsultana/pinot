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

import org.apache.calcite.pinot.traits.PinotRelDistribution;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.SortExchange;


public class PinotSortExchange extends SortExchange {
  private final boolean _isIdentityDistribution;
  private final boolean _isIdentityCollation;

  PinotSortExchange(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, RelDistribution newDistribution,
      RelCollation collation, boolean isIdentityDistribution, boolean isIdentityCollation) {
    super(cluster, traitSet, input, newDistribution, collation);
    _isIdentityCollation = isIdentityCollation;
    _isIdentityDistribution = isIdentityDistribution;
  }

  @Override public SortExchange copy(RelTraitSet traitSet, RelNode newInput,
      RelDistribution newDistribution, RelCollation newCollation) {
    return new PinotSortExchange(this.getCluster(), traitSet.plus(newDistribution).plus(newCollation), newInput,
        newDistribution, newCollation, false, false);
  }

  public PinotRelDistribution getPinotRelDistribution() {
    return (PinotRelDistribution) this.distribution;
  }

  public static PinotSortExchange create(SortExchange sortExchange) {
    PinotRelDistribution pinotRelDistribution = PinotRelDistribution.create(sortExchange.getDistribution());
    RelTraitSet newTraitSet = RelTraitSet.createEmpty();
    for (RelTrait relTrait : sortExchange.getTraitSet()) {
      if (!relTrait.getTraitDef().equals(RelDistributionTraitDef.INSTANCE)) {
        newTraitSet = newTraitSet.plus(relTrait);
      }
    }
    return new PinotSortExchange(sortExchange.getCluster(), newTraitSet.plus(pinotRelDistribution),
        sortExchange.getInput(), pinotRelDistribution, sortExchange.getCollation(), false, false);
  }

  public static PinotSortExchange create(RelNode input, PinotRelDistribution relDistribution, RelCollation collation) {
    return new PinotSortExchange(input.getCluster(), input.getTraitSet().plus(relDistribution), input, relDistribution,
        collation, false, false);
  }

  public static PinotSortExchange create(RelNode input, PinotRelDistribution relDistribution, RelCollation collation,
      boolean isIdentityDistribution, boolean isIdentityCollation) {
    return new PinotSortExchange(input.getCluster(), input.getTraitSet(), input, relDistribution, collation,
        isIdentityDistribution, isIdentityCollation);
  }
}
