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
import java.util.Objects;
import java.util.Set;
import org.apache.calcite.pinot.mappings.GeneralMapping;
import org.apache.calcite.pinot.traits.PinotRelDistribution;
import org.apache.calcite.pinot.traits.PinotRelDistributionTraitDef;
import org.apache.calcite.pinot.traits.PinotRelDistributions;
import org.apache.calcite.plan.PinotTraitUtils;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.commons.lang3.tuple.Pair;


public class ExchangeFactory {
  private ExchangeFactory() {
  }

  public static Pair<PinotExchange, PinotExchange> create(Join join) {
    Pair<Set<PinotRelDistribution>, Set<PinotRelDistribution>> joinDistributions =
        PinotTraitUtils.getJoinDistributions(join);
    PinotExchange leftExchange = null;
    RelDistribution.Type leftDistributionType = joinDistributions.getLeft().iterator().next().getType();
    if (leftDistributionType.equals(RelDistribution.Type.HASH_DISTRIBUTED)) {
      boolean allTraitsSatisfied = true;
      for (PinotRelDistribution requiredLeftDistribution : joinDistributions.getLeft()) {
        boolean isTraitSatisfied = join.getLeft().getTraitSet()
            .stream().anyMatch(inputTrait -> inputTrait.satisfies(requiredLeftDistribution));
        if (!isTraitSatisfied) {
          allTraitsSatisfied = false;
          break;
        }
      }
      if (allTraitsSatisfied) {
        leftExchange = PinotExchange.createIdentity(join.getLeft());
      } else {
        Preconditions.checkState(joinDistributions.getLeft().size() == 1);
        leftExchange = PinotExchange.create(join.getLeft(), joinDistributions.getLeft().iterator().next());
      }
    } else if (leftDistributionType.equals(RelDistribution.Type.RANDOM_DISTRIBUTED)) {
      leftExchange = PinotExchange.create(join.getLeft(), PinotRelDistributions.RANDOM);
    } else {
      throw new IllegalStateException(String.format("Found unexpected distribution for left-child of join: %s",
          leftDistributionType));
    }

    PinotExchange rightExchange = null;
    RelDistribution.Type rightDistributionType = joinDistributions.getRight().iterator().next().getType();
    if (rightDistributionType.equals(RelDistribution.Type.HASH_DISTRIBUTED)) {
      GeneralMapping rightMapping = GeneralMapping.infer(join).getRight().inverse();
      Mappings.TargetMapping joinToRightInput = rightMapping.asTargetMapping();
      boolean allTraitsSatisfied = true;
      for (PinotRelDistribution rightDistribution : joinDistributions.getRight()) {
        PinotRelDistribution requiredRightDistribution = rightDistribution.apply(joinToRightInput);
        boolean isTraitSatisfied = join.getRight().getTraitSet()
            .stream().anyMatch(inputTrait -> inputTrait.satisfies(requiredRightDistribution));
        if (!isTraitSatisfied) {
          allTraitsSatisfied = false;
          break;
        }
      }
      if (allTraitsSatisfied) {
        rightExchange = PinotExchange.createIdentity(join.getRight());
      } else {
        Preconditions.checkState(joinDistributions.getRight().size() == 1);
        PinotRelDistribution exchangeDistribution =
            joinDistributions.getRight().stream().iterator().next().apply(joinToRightInput);
        rightExchange = PinotExchange.create(join.getRight(), exchangeDistribution);
      }
    } else if (rightDistributionType.equals(RelDistribution.Type.BROADCAST_DISTRIBUTED)) {
      rightExchange = PinotExchange.create(join.getRight(), PinotRelDistributions.BROADCAST);
    } else {
      throw new IllegalStateException(String.format("Found unexpected distribution for right-child of join: %s",
          rightDistributionType));
    }
    return Pair.of(leftExchange, rightExchange);
  }

  public static PinotExchange create(Aggregate aggregate) {
     PinotRelDistribution distribution = Objects.requireNonNull(aggregate.getTraitSet().getDistribution());
     if (distribution.getType().equals(RelDistribution.Type.SINGLETON)) {
       return PinotExchange.create(aggregate.getInput(), PinotRelDistributions.SINGLETON);
     }
     Preconditions.checkState(distribution.getType().equals(RelDistribution.Type.HASH_DISTRIBUTED),
         String.format("Found unexpected distribution-type for aggregate: %s", distribution.getType()));
     List<PinotRelDistribution> distributionTraits =
         Objects.requireNonNull(aggregate.getTraitSet().getTraits(PinotRelDistributionTraitDef.INSTANCE));
     Mappings.TargetMapping aggToInput = GeneralMapping.infer(aggregate).inverse().asTargetMapping();
     boolean allTraitsSatisfied = true;
     for (PinotRelDistribution aggDistribution : distributionTraits) {
       PinotRelDistribution requiredTrait = aggDistribution.apply(aggToInput);
       Preconditions.checkNotNull(requiredTrait, "Indicates a bug in setting traits");
       if (aggregate.getInput().getTraitSet().stream().noneMatch(inputTrait -> inputTrait.satisfies(requiredTrait))) {
         allTraitsSatisfied = false;
         break;
       }
     }
     if (allTraitsSatisfied) {
       return PinotExchange.createIdentity(aggregate.getInput());
     }
     Preconditions.checkState(distributionTraits.size() == 1);
     return PinotExchange.create(aggregate.getInput(), distributionTraits.get(0).apply(aggToInput));
  }

  public static Exchange create(Window window) {
    throw new UnsupportedOperationException("TODO");
  }
}
