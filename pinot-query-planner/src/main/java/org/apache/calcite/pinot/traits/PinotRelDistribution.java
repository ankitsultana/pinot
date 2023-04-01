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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.calcite.plan.RelMultipleTrait;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.util.mapping.Mappings;


public class PinotRelDistribution implements RelDistribution {
  private static final Ordering<Iterable<Integer>> ORDERING =
      Ordering.<Integer>natural().lexicographical();
  private static final List<Type> VALID_TYPES = ImmutableList.of(Type.HASH_DISTRIBUTED, Type.ANY, Type.SINGLETON,
      Type.RANDOM_DISTRIBUTED, Type.BROADCAST_DISTRIBUTED);

  private final List<Integer> _keys;
  @Nullable
  private final Integer _numPartitions;
  @Nullable
  private final String _functionName;
  private final Type _type;

  PinotRelDistribution(List<Integer> keys, Integer numPartitions, String functionName, Type type) {
    Preconditions.checkState(VALID_TYPES.contains(type));
    _keys = keys;
    _numPartitions = numPartitions;
    _functionName = functionName;
    _type = type;
  }

  @Override
  public Type getType() {
    return _type;
  }

  @Override
  public List<Integer> getKeys() {
    return _keys;
  }

  @Override
  public PinotRelDistribution apply(@Nullable Mappings.TargetMapping mapping) {
    if (_keys.isEmpty()) {
      return this;
    } else if (mapping == null) {
      return PinotRelDistributions.ANY;
    }
    for (int key : _keys) {
      if (mapping.getTargetOpt(key) == -1) {
        return PinotRelDistributions.ANY;
      }
    }
    List<Integer> mappedKeys = _keys.stream().map(mapping::getTargetOpt).collect(Collectors.toList());
    return new PinotRelDistribution(mappedKeys, _numPartitions, _functionName, _type);
  }

  @Override
  public boolean isTop() {
    return false;
  }

  @Override
  public int compareTo(@Nonnull RelMultipleTrait o) {
    final PinotRelDistribution distribution = (PinotRelDistribution) o;
    if (_type == distribution.getType()
        && (_type == Type.HASH_DISTRIBUTED
            || _type == Type.RANGE_DISTRIBUTED)) {
      return ORDERING.compare(_keys, distribution.getKeys());
    }
    return _type.compareTo(distribution.getType());
  }

  @Override
  public RelTraitDef getTraitDef() {
    return PinotRelDistributionTraitDef.INSTANCE;
  }

  @Override
  public boolean satisfies(RelTrait trait) {
    Preconditions.checkState(trait instanceof PinotRelDistribution);
    PinotRelDistribution otherTrait = (PinotRelDistribution) trait;
    if (otherTrait.getType().equals(RelDistribution.Type.ANY)) {
      return true;
    } else if (otherTrait.getType().equals(Type.HASH_DISTRIBUTED)) {
      if (_type.equals(Type.HASH_DISTRIBUTED)) {
        Preconditions.checkState(_numPartitions != null && _functionName != null);
        Preconditions.checkState(otherTrait._numPartitions != null);
        return otherTrait.getKeys().containsAll(_keys)
            && (otherTrait._numPartitions == -1 || _numPartitions.equals(otherTrait._numPartitions))
            && _functionName.equalsIgnoreCase(otherTrait._functionName);
      }
      return false;
    } else if (_type.equals(Type.BROADCAST_DISTRIBUTED)) {
      return otherTrait.getType().equals(Type.BROADCAST_DISTRIBUTED);
    }
    return false;
  }

  @Override
  public void register(RelOptPlanner planner) {
  }

  public int getNumPartitions() {
    return _numPartitions;
  }

  @Override
  public String toString() {
    if (_type.equals(Type.HASH_DISTRIBUTED)) {
      return String.format("hash%s", _keys);
    } else {
      return _type.shortName;
    }
  }

  public static PinotRelDistribution create(RelDistribution relDistribution) {
    if (relDistribution.getType().equals(Type.BROADCAST_DISTRIBUTED)) {
      return PinotRelDistributions.BROADCAST;
    } else if (relDistribution.getType().equals(Type.ANY)) {
      return PinotRelDistributions.ANY;
    } else if (relDistribution.getType().equals(Type.RANDOM_DISTRIBUTED)) {
      return PinotRelDistributions.RANDOM;
    } else if (relDistribution.getType().equals(Type.HASH_DISTRIBUTED)) {
      return PinotRelDistributions.hash(relDistribution.getKeys(), -1);
    }
    throw new IllegalStateException(String.format("Found dist: %s", relDistribution.getType()));
  }
}
