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
import javax.annotation.Nullable;
import org.apache.calcite.pinot.traits.PinotRelDistribution;
import org.apache.calcite.pinot.traits.PinotRelDistributionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.commons.collections.CollectionUtils;


public class PinotExchange extends SingleRel {
  private final boolean _isIdentity;
  @Nullable private PinotRelDistribution _distribution;

  PinotExchange(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, @Nullable PinotRelDistribution distribution,
      boolean isIdentity) {
    super(cluster, traitSet, input);
    _isIdentity = isIdentity;
    _distribution = distribution;
  }

  @Override
  public PinotExchange copy(RelTraitSet traitSet, List<RelNode> inputs) {
    RelNode newInput = inputs.get(0);
    List<PinotRelDistribution> pinotRelDistributions = traitSet.getTraits(PinotRelDistributionTraitDef.INSTANCE);
    PinotRelDistribution pinotRelDistribution = null;
    if (CollectionUtils.isNotEmpty(pinotRelDistributions) && pinotRelDistributions.size() == 1) {
      pinotRelDistribution = pinotRelDistributions.get(0);
    }
    return new PinotExchange(getCluster(), traitSet, newInput, pinotRelDistribution, _isIdentity);
  }

  @Nullable
  public PinotRelDistribution getDistribution() {
    return _distribution;
  }

  public boolean isIdentity() {
    return _isIdentity;
  }

  public static PinotExchange create(RelNode input, PinotRelDistribution relDistribution) {
    return new PinotExchange(input.getCluster(), input.getTraitSet().plus(relDistribution), input, relDistribution,
        false);
  }

  public static PinotExchange createIdentity(RelNode input) {
    return new PinotExchange(input.getCluster(), input.getTraitSet(), input, null, true);
  }

  @Override public RelWriter explainTerms(RelWriter pw) {
    // return super.explainTerms(pw).item("identity", _isIdentity);
    return super.explainTerms(pw).item("distribution", _distribution == null ? "identity" : _distribution);
  }
}
