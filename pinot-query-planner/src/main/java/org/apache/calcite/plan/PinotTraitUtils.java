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
package org.apache.calcite.plan;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.calcite.pinot.traits.PinotRelDistribution;
import org.apache.calcite.pinot.traits.PinotRelDistributionTraitDef;
import org.apache.calcite.pinot.traits.PinotRelDistributions;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;


public class PinotTraitUtils {

  private PinotTraitUtils() {
  }

  @SuppressWarnings("unchecked")
  public static RelTraitSet toRelTraitSet(Set<RelTrait> traits) {
    RelTraitSet relTraitSet = RelTraitSet.createEmpty();
    Map<RelTraitDef<RelTrait>, List<RelMultipleTrait>> relMultipleTraitMap = new HashMap<>();
    for (RelTrait trait : traits) {
      if (trait instanceof RelCompositeTrait) {
        Preconditions.checkState(!relMultipleTraitMap.containsKey(trait.getTraitDef()));
        relMultipleTraitMap.put(trait.getTraitDef(), ((RelCompositeTrait<RelMultipleTrait>) trait).traitList());
      } else if (trait instanceof RelMultipleTrait) {
        relMultipleTraitMap.computeIfAbsent(trait.getTraitDef(), (x) -> new ArrayList<>());
        relMultipleTraitMap.get(trait.getTraitDef()).add((RelMultipleTrait) trait);
      } else {
        relTraitSet = relTraitSet.plus(trait);
      }
    }
    for (List<RelMultipleTrait> multipleTraits : relMultipleTraitMap.values()) {
      List<RelMultipleTrait> sortedTraits = multipleTraits.stream().sorted().collect(Collectors.toList());
      relTraitSet = relTraitSet.plus(RelCompositeTrait.of(multipleTraits.get(0).getTraitDef(), sortedTraits));
    }
    return relTraitSet;
  }

  public static RelTraitSet apply(RelTraitSet inputTraits, Mappings.TargetMapping mapping) {
    RelTraitSet relTraitSet = RelTraitSet.createEmpty();
    boolean added = false;
    List<PinotRelDistribution> pinotDistributions =
        Objects.requireNonNull(inputTraits.getTraits(PinotRelDistributionTraitDef.INSTANCE));
    List<PinotRelDistribution> newDistributions = new ArrayList<>();
    for (RelTrait pinotDistribution : pinotDistributions) {
      PinotRelDistribution newDistribution = pinotDistribution.apply(mapping);
      if (!newDistribution.getType().equals(RelDistribution.Type.ANY)) {
        added = true;
        newDistributions.add(newDistribution);
      }
    }
    if (!added) {
      newDistributions.add(PinotRelDistributions.ANY);
    }
    Collections.sort(newDistributions);
    relTraitSet = relTraitSet.plus(RelCompositeTrait.of(PinotRelDistributionTraitDef.INSTANCE, newDistributions));
    for (RelTrait trait : inputTraits) {
      if (!trait.getTraitDef().equals(PinotRelDistributionTraitDef.INSTANCE)) {
        relTraitSet = relTraitSet.plus(trait);
      }
    }
    return relTraitSet;
  }

  public static Set<RelTrait> unwrapRelCompositeTraits(RelTraitSet relTraitSet) {
    Set<RelTrait> result = new HashSet<>();
    for (RelTrait trait : relTraitSet) {
      result.addAll(unwrapRelCompositeTrait(trait));
    }
    return result;
  }

  @SuppressWarnings("unchecked")
  public static Set<RelTrait> unwrapRelCompositeTrait(RelTrait trait) {
    Set<RelTrait> result = new HashSet<>();
    if (trait instanceof RelCompositeTrait) {
      result.addAll(((RelCompositeTrait<RelMultipleTrait>) trait).traitList());
    } else {
      result.add(trait);
    }
    return result;
  }

  public static RelTraitSet resetDistribution(RelTraitSet relTraitSet, PinotRelDistribution distribution) {
    RelTraitSet resultTraits = RelTraitSet.createEmpty();
    for (RelTrait trait : relTraitSet) {
      if (!trait.getTraitDef().equals(PinotRelDistributionTraitDef.INSTANCE)) {
        resultTraits = resultTraits.plus(trait);
      }
    }
    return resultTraits.plus(RelCompositeTrait.of(PinotRelDistributionTraitDef.INSTANCE,
        Collections.singletonList(distribution)));
  }

  public static Set<PinotRelDistribution> asSet(RelTraitSet relTraitSet) {
    Set<PinotRelDistribution> result = new HashSet<>();
    for (RelTrait trait : relTraitSet) {
      if (trait.getTraitDef().equals(PinotRelDistributionTraitDef.INSTANCE)) {
        result.addAll(unwrapDistributions(trait));
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
        List<PinotRelDistribution> distributions = unwrapDistributions(trait);
        for (PinotRelDistribution pinotDistribution : distributions) {
          Preconditions.checkState(CollectionUtils.isNotEmpty(pinotDistribution.getKeys()));
          if (pinotDistribution.getKeys().stream().min(Integer::compareTo).orElseThrow() >= leftFieldCount) {
            rightDistributions.add(pinotDistribution);
          } else {
            leftDistributions.add(pinotDistribution);
          }
        }
      }
    }
    return Pair.of(leftDistributions, rightDistributions);
  }

  public static RelTraitSet removeCollations(RelTraitSet relTraitSet) {
    return filter(relTraitSet, (x) -> !x.getTraitDef().equals(RelCollationTraitDef.INSTANCE));
  }

  private static RelTraitSet filter(RelTraitSet relTraitSet, Predicate<RelTrait> predicate) {
    RelTraitSet result = RelTraitSet.createEmpty();
    for (RelTrait trait : relTraitSet) {
      if (predicate.test(trait)) {
        result = result.plus(trait);
      }
    }
    return result;
  }

  @SuppressWarnings("unchecked")
  private static List<PinotRelDistribution> unwrapDistributions(RelTrait relTrait) {
    if (relTrait instanceof RelCompositeTrait) {
      return ((RelCompositeTrait<PinotRelDistribution>) relTrait).traitList();
    }
    return Collections.singletonList((PinotRelDistribution) relTrait);
  }
}
