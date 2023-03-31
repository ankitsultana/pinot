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
package org.apache.calcite.pinot.traits;

import com.google.common.base.Preconditions;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Predicate;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;


public class PinotTraitUtils {

  private PinotTraitUtils() {
  }

  public static RelTraitSet apply(RelTraitSet inputTraits, Mappings.TargetMapping mapping) {
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

  public static RelTraitSet filter(RelTraitSet relTraitSet, Predicate<RelTrait> predicate) {
    RelTraitSet result = RelTraitSet.createEmpty();
    for (RelTrait trait : relTraitSet) {
      if (predicate.test(trait)) {
        result = result.plus(trait);
      }
    }
    return result;
  }

  public static RelTraitSet setDistribution(RelTraitSet relTraitSet, PinotRelDistribution distribution) {
    RelTraitSet resultTraits = RelTraitSet.createEmpty();
    for (RelTrait trait : relTraitSet) {
      if (!trait.getTraitDef().equals(PinotRelDistributionTraitDef.INSTANCE)) {
        resultTraits = resultTraits.plus(trait);
      }
    }
    return resultTraits.plus(distribution);
  }

  public static Set<PinotRelDistribution> asSet(RelTraitSet relTraitSet) {
    Set<PinotRelDistribution> result = new HashSet<>();
    for (RelTrait trait : relTraitSet) {
      if (trait.getTraitDef().equals(PinotRelDistributionTraitDef.INSTANCE)) {
        result.add((PinotRelDistribution) trait);
      }
    }
    return result;
  }

  public static Pair<Set<PinotRelDistribution>, Set<PinotRelDistribution>> getJoinDistributions(Join join) {
    Set<PinotRelDistribution> leftDistributions = new HashSet<>();
    Set<PinotRelDistribution> rightDistributions = new HashSet<>();
    int leftFieldCount = join.getLeft().getRowType().getFieldCount();
    for (RelTrait trait : join.getTraitSet()) {
      if (trait.getTraitDef().equals(PinotRelDistributionTraitDef.INSTANCE)) {
        PinotRelDistribution pinotDistribution = (PinotRelDistribution) trait;
        Preconditions.checkState(CollectionUtils.isNotEmpty(pinotDistribution.getKeys()));
        if (pinotDistribution.getKeys().stream().min(Integer::compareTo).orElseThrow() >= leftFieldCount) {
          rightDistributions.add(pinotDistribution);
        } else {
          leftDistributions.add(pinotDistribution);
        }
      }
    }
    return Pair.of(leftDistributions, rightDistributions);
  }

  public static RelTraitSet removeCollations(RelTraitSet relTraitSet) {
    return filter(relTraitSet, (x) -> !x.getTraitDef().equals(RelCollationTraitDef.INSTANCE));
  }
}
