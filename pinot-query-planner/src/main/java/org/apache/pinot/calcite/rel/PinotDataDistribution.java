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
package org.apache.pinot.calcite.rel;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.util.Util;
import org.apache.commons.lang3.tuple.Pair;


public class PinotDataDistribution {
  private final Type _type;
  private final List<String> _workers;
  private final long _workerHash;
  @Nullable
  private final Set<HashDistributionDesc> _hashDistributionDesc;
  @Nullable
  private final List<Integer> _collationKeys;
  @Nullable
  private final List<RelFieldCollation> _fieldCollationDirection;

  public PinotDataDistribution(Type type, List<String> workers, long workerHash,
      @Nullable Set<HashDistributionDesc> desc, @Nullable List<Integer> collationKeys,
      @Nullable List<RelFieldCollation> fieldCollationDirection) {
    _type = type;
    _workers = workers;
    _workerHash = workerHash;
    _hashDistributionDesc = desc;
    _collationKeys = collationKeys;
    _fieldCollationDirection = fieldCollationDirection;
    validate();
  }

  public Type getType() {
    return _type;
  }

  public List<String> getWorkers() {
    return _workers;
  }

  public long getWorkerHash() {
    return _workerHash;
  }

  @Nullable
  public Set<HashDistributionDesc> getHashDistributionDesc() {
    return _hashDistributionDesc;
  }

  @Nullable
  public List<Integer> getCollationKeys() {
    return _collationKeys;
  }

  @Nullable
  public List<RelFieldCollation> getFieldCollationDirection() {
    return _fieldCollationDirection;
  }

  /**
   * <pre>
   *   Input: distribution required logically for correctness.
   *   Output: whether this physical distribution meets the input constraint.
   * </pre>
   */
  public boolean satisfies(@Nullable RelDistribution distributionConstraint) {
    if (distributionConstraint == null) {
      return true;
    }
    if (distributionConstraint == RelDistributions.ANY
        || distributionConstraint == RelDistributions.RANDOM_DISTRIBUTED) {
      return true;
    } else if (distributionConstraint == RelDistributions.BROADCAST_DISTRIBUTED) {
      // TODO: We could do better when the input node is only on a single worker.
      return _type == Type.BROADCAST;
    } else if (distributionConstraint == RelDistributions.SINGLETON) {
      return _type == Type.SINGLETON;
    }
    if (distributionConstraint.getType() != RelDistribution.Type.HASH_DISTRIBUTED) {
      throw new IllegalStateException("Unexpected rel distribution type: " + distributionConstraint.getType());
    }
    if (_type != Type.HASH_PARTITIONED) {
      return false;
    }
    HashDistributionDesc hashDistributionDesc = satisfiesHashDistributionConstraint(distributionConstraint);
    return false;
  }

  public boolean satisfies(@Nullable RelCollation relCollation) {
    if (relCollation == null || relCollation == RelCollations.EMPTY) {
      return true;
    }
    if (relCollation.getKeys().isEmpty()) {
      return true;
    }
    if (_collationKeys == null || _collationKeys.isEmpty()) {
      return false;
    }
    if (_fieldCollationDirection == null) {
      return false;
    }
    return Util.startsWith(_collationKeys, relCollation.getKeys()) && Util.startsWith(
        _fieldCollationDirection, relCollation.getFieldCollations()));
  }

  public boolean satisfies(RelDistribution... distributionConstraints) {
    for (RelDistribution distributionConstraint : distributionConstraints) {
      if (!satisfies(distributionConstraint)) {
        return false;
      }
    }
    return true;
  }

  public PinotDataDistribution apply(Map<Integer, Integer> mapping) {
    if (_hashDistributionDesc != null) {
      for (HashDistributionDesc desc : _hashDistributionDesc) {
      }
    }
  }

  @Nullable
  public HashDistributionDesc satisfiesHashDistributionConstraint(RelDistribution hashConstraint) {
    Preconditions.checkNotNull(_hashDistributionDesc, "Found hashDistributionDesc null in satisfies");
    /* // once we add support for non-strict distributions, we can just check partial distributions.
    for (HashDistributionDesc desc : _hashDistributionDesc) {
      if (new HashSet<>(desc._keyIndexes).containsAll(hashConstraint.getKeys())) {
        return desc;
      }
    } */
    return _hashDistributionDesc.stream().filter(x -> x.getKeyIndexes().equals(hashConstraint.getKeys())).findFirst().orElse(null);
    // Calcite considers ordering of keys un-important. But now we only do ordered comparison.
  }

  @Nullable
  private Pair<List<Integer>, List<RelFieldCollation>> apply(Map<Integer, Integer> mapping) {
    if (_collationKeys == null) {
      return null;
    }
    for (Integer currentCollationIndex : _collationKeys) {
      if (!mapping.containsKey(currentCollationIndex)) {
      }
    }
  }

  public enum Type {
    SINGLETON,
    HASH_PARTITIONED,
    BROADCAST,
    RANDOM
  }

  public static class HashDistributionDesc {
    List<Integer> _keyIndexes = Collections.emptyList();
    String _hashFunction;
    int _numPartitions;
    int _subPartitioningFactor;

    public HashDistributionDesc() {
    }

    public HashDistributionDesc(List<Integer> keyIndexes, String hashFunction, int numPartitions, int subPartitioningFactor) {
      _keyIndexes = keyIndexes;
      _hashFunction = hashFunction;
      _numPartitions = numPartitions;
      _subPartitioningFactor = subPartitioningFactor;
    }

    public List<Integer> getKeyIndexes() {
      return _keyIndexes;
    }

    public String getHashFunction() {
      return _hashFunction;
    }

    public int getNumPartitions() {
      return _numPartitions;
    }

    public int getSubPartitioningFactor() {
      return _subPartitioningFactor;
    }

    @Nullable
    HashDistributionDesc apply(Map<Integer, Integer> mapping) {
      List<Integer> result = new ArrayList<>(_keyIndexes.size());
      for (Integer currentKeyIndex : _keyIndexes) {
        if (!mapping.containsKey(currentKeyIndex)) {
          return null;
        }
        result.add(mapping.get(currentKeyIndex));
      }
      HashDistributionDesc desc = new HashDistributionDesc();
      desc._keyIndexes = result;
      desc._hashFunction = _hashFunction;
      desc._numPartitions = _numPartitions;
      desc._subPartitioningFactor = _subPartitioningFactor;
      return desc;
    }
  }

  private void validate() {
    if (_type == Type.SINGLETON && _workers.size() > 1) {
      throw new IllegalStateException(String.format("Singleton distribution with %s workers", _workers.size()));
    }
    if (_collationKeys != null) {
      Preconditions.checkNotNull(_fieldCollationDirection);
      Preconditions.checkState(_collationKeys.size() == _fieldCollationDirection.size());
    }
  }
}
