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
import java.util.Objects;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.logical.LogicalExchange;


public class PinotExchange extends Exchange {
  private final boolean _isIdentity;

  PinotExchange(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, boolean isIdentity) {
    super(cluster, traitSet, input, Objects.requireNonNull(traitSet.getTrait(PinotRelDistributionTraitDef.INSTANCE)));
    Preconditions.checkState(distribution instanceof PinotRelDistribution);
    _isIdentity = isIdentity;
  }

  @Override
  public Exchange copy(RelTraitSet traitSet, RelNode newInput, RelDistribution newDistribution) {
    traitSet = traitSet.plus(newDistribution);
    return new PinotExchange(getCluster(), traitSet, newInput, _isIdentity);
  }

  public boolean isIdentity() {
    return _isIdentity;
  }

  public static PinotExchange create(LogicalExchange logicalExchange) {
    PinotRelDistribution pinotRelDistribution = PinotRelDistribution.create(logicalExchange.getDistribution());
    RelTraitSet newTraitSet = RelTraitSet.createEmpty();
    for (RelTrait relTrait : logicalExchange.getTraitSet()) {
      if (!relTrait.getTraitDef().equals(RelDistributionTraitDef.INSTANCE)) {
        newTraitSet = newTraitSet.plus(relTrait);
      }
    }
    return new PinotExchange(logicalExchange.getCluster(), newTraitSet.plus(pinotRelDistribution),
        logicalExchange.getInput(), false);
  }

  public static PinotExchange create(RelNode input, PinotRelDistribution relDistribution) {
    return new PinotExchange(input.getCluster(), input.getTraitSet().plus(relDistribution), input, false);
  }

  public static PinotExchange createIdentity(RelNode input) {
    return new PinotExchange(input.getCluster(), input.getTraitSet(), input, true);
  }

  @Override public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw).item("identity", _isIdentity);
  }
}
