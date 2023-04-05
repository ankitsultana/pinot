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
import java.util.List;
import javax.annotation.Nullable;
import org.apache.calcite.pinot.traits.PinotRelDistribution;
import org.apache.calcite.pinot.traits.PinotRelDistributionTraitDef;
import org.apache.calcite.plan.PinotTraitUtils;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.commons.collections.CollectionUtils;


public class PinotSortExchange extends PinotExchange {
  private final RelCollation _collation;

  PinotSortExchange(RelOptCluster cluster, RelTraitSet traitSet, RelNode input,
      @Nullable PinotRelDistribution newDistribution, RelCollation collation) {
    super(cluster, traitSet, input, newDistribution);
    _collation = collation;
  }

  @Override public PinotSortExchange copy(RelTraitSet traitSet, List<RelNode> inputs) {
    RelCollation collation = traitSet.getCollation();
    List<PinotRelDistribution> distributions = traitSet.getTraits(PinotRelDistributionTraitDef.INSTANCE);
    PinotRelDistribution newDistribution = null;
    if (CollectionUtils.isNotEmpty(distributions)) {
      Preconditions.checkState(distributions.size() == 1);
      newDistribution = distributions.iterator().next();
    }
    return new PinotSortExchange(getCluster(), traitSet, inputs.get(0), newDistribution, collation);
  }

  public PinotSortExchange copy(RelTraitSet traitSet, RelNode input, PinotRelDistribution distribution) {
    traitSet = traitSet.plus(_collation);
    traitSet = PinotTraitUtils.resetDistribution(traitSet, distribution);
    return new PinotSortExchange(input.getCluster(), traitSet, input, distribution, _collation);
  }

  public RelCollation getCollation() {
    return _collation;
  }

  @Override public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw).item("collation", _collation);
  }

  public static PinotSortExchange create(RelNode input, PinotRelDistribution relDistribution, RelCollation collation) {
    RelTraitSet newTraits = PinotTraitUtils.resetDistribution(input.getTraitSet(), relDistribution);
    newTraits = newTraits.plus(collation);
    return new PinotSortExchange(input.getCluster(), newTraits, input, relDistribution, collation);
  }

  public static PinotSortExchange createIdentityDistribution(RelNode input, RelCollation collation) {
    return new PinotSortExchange(input.getCluster(), input.getTraitSet(), input, null, collation);
  }
}
