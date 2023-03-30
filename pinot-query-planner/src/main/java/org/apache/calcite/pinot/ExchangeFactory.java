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
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.pinot.traits.PinotRelDistribution;
import org.apache.calcite.pinot.traits.PinotRelDistributionTraitDef;
import org.apache.calcite.pinot.traits.PinotTraitUtils;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Exchange;


public class ExchangeFactory {
  private ExchangeFactory() {
  }

  public static Exchange create(RelNode receiver, RelNode sender) {
    List<RelTrait> unsatisfiedTraits = new ArrayList<>();
    for (RelTrait receiverTrait : receiver.getTraitSet()) {
      if (sender.getTraitSet().stream().noneMatch(receiverTrait::satisfies)) {
        unsatisfiedTraits.add(receiverTrait);
      }
    }
    // TODO: We don't do identity exchanges when RelCollation trait is present.
    if (receiver.getTraitSet().stream().anyMatch(x -> x.getTraitDef().equals(RelCollationTraitDef.INSTANCE))) {
      for (RelTrait trait : receiver.getTraitSet()) {
      }
      // Needs to be a PinotSortExchange
    }
    if (unsatisfiedTraits.isEmpty()) {
      return PinotExchange.createIdentity(sender);
    }
    Preconditions.checkState(receiver.getTraitSet().size() == 1);
    Preconditions.checkState(receiver.getTraitSet().stream()
        .allMatch(x -> x.getTraitDef().equals(PinotRelDistributionTraitDef.INSTANCE)));
    PinotRelDistribution onlyTrait =
        receiver.getTraitSet().stream().map(x -> (PinotRelDistribution) x).collect(Collectors.toList()).get(0);
    return new PinotExchange(receiver.getCluster(), );
  }
}
