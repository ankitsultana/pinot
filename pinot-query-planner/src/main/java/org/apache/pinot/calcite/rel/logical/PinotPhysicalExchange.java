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
package org.apache.pinot.calcite.rel.logical;

import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Exchange;
import org.apache.pinot.calcite.rel.PinotPhysicalExchangeType;


public class PinotPhysicalExchange extends Exchange {
  private final List<Integer> _keys;
  private final PinotPhysicalExchangeType _exchangeStrategy;
  private final RelCollation _collation;

  public PinotPhysicalExchange(RelNode input, List<Integer> keys, PinotPhysicalExchangeType exchangeStrategy) {
    this(input, keys, exchangeStrategy, null);
  }

  // TODO: Using random distributed here is a hack.
  public PinotPhysicalExchange(RelNode input, List<Integer> keys, PinotPhysicalExchangeType desc,
      RelCollation collation) {
    super(input.getCluster(), RelTraitSet.createEmpty().plus(RelDistributions.RANDOM_DISTRIBUTED), input,
        RelDistributions.RANDOM_DISTRIBUTED);
    _keys = keys;
    _exchangeStrategy = desc;
    _collation = collation == null ? RelCollations.EMPTY : collation;
  }

  public static PinotPhysicalExchange broadcast(RelNode input) {
    return new PinotPhysicalExchange(input, Collections.emptyList(), PinotPhysicalExchangeType.BROADCAST_EXCHANGE,
        null);
  }

  public static PinotPhysicalExchange singleton(RelNode input) {
    return new PinotPhysicalExchange(input, Collections.emptyList(), PinotPhysicalExchangeType.SINGLETON_EXCHANGE,
        null);
  }

  @Override
  public Exchange copy(RelTraitSet traitSet, RelNode newInput, RelDistribution newDistribution) {
    // TODO: I m dropping the trait set here. That's a hack.
    Preconditions.checkState(traitSet.size() <= 1, "At most 1 trait allowed in PinotPhysicalExc");
    Preconditions.checkState(traitSet.isEmpty() || traitSet.getDistribution() != null,
        "Only distribution trait allowed in PinotPhysicalExchange");
    return new PinotPhysicalExchange(newInput, _keys, _exchangeStrategy, _collation);
  }

  public List<Integer> getKeys() {
    return _keys;
  }

  public PinotPhysicalExchangeType getExchangeStrategy() {
    return _exchangeStrategy;
  }

  public RelCollation getCollation() {
    return _collation;
  }

  @Override
  public String getRelTypeName() {
    return String.format("PinotPhysicalExchange(strategy=%s, keys=%s)", _exchangeStrategy, _keys);
  }
}
