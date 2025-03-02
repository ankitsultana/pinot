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
import java.util.Comparator;
import java.util.HashSet;
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
import org.apache.calcite.util.mapping.Mappings;


public class PinotDataDistribution {
  private final Type _type;
  private final List<String> _workers;
  private final long _workerHash;
  @Nullable
  private final Set<HashDistributionDesc> _hashDistributionDesc;
  private final RelCollation _collation;

  public PinotDataDistribution(Type type, List<String> workers, long workerHash,
      @Nullable Set<HashDistributionDesc> desc, @Nullable RelCollation collation) {
    _type = type;
    _workers = workers;
    _workerHash = workerHash;
    _hashDistributionDesc = desc;
    _collation = collation == null ? RelCollations.EMPTY : collation;
    validate();
  }

  public PinotDataDistribution withCollation(RelCollation collation) {
    return new PinotDataDistribution(_type, _workers, _workerHash, _hashDistributionDesc, collation);
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

  public List<Integer> getCollationKeys() {
    return _collation.getKeys();
  }

  public List<RelFieldCollation> getFieldCollationDirection() {
    return _collation.getFieldCollations();
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
    return hashDistributionDesc != null;
  }

  public boolean satisfies(@Nullable RelCollation relCollation) {
    if (relCollation == null || relCollation == RelCollations.EMPTY || relCollation.getKeys().isEmpty()) {
      return true;
    }
    if (_collation == null) {
      return false;
    }
    return _collation.satisfies(relCollation);
  }

  public PinotDataDistribution apply(@Nullable Map<Integer, Integer> mapping) {
    if (mapping == null) {
      return new PinotDataDistribution(Type.RANDOM, _workers, _workerHash, null, null);
    }
    // TODO(ankitsultana-correctness): Review this.
    Set<HashDistributionDesc> newHashDesc =  new HashSet<>();
    if (_hashDistributionDesc != null) {
      for (HashDistributionDesc desc : _hashDistributionDesc) {
        List<Integer> newKeys = new ArrayList<>();
        for (Integer currentKey : desc.getKeyIndexes()) {
          if (mapping.containsKey(currentKey) && mapping.get(currentKey) >= 0) {
            newKeys.add(mapping.get(currentKey));
          }
        }
        if (newKeys.size() == desc.getKeyIndexes().size()) {
          newHashDesc.add(new HashDistributionDesc(newKeys, desc._hashFunction, desc.getNumPartitions(), desc._subPartitioningFactor));
        }
      }
    }
    List<Integer> mpListKeys = createMappingList(mapping);
    int targetCount = mpListKeys.stream().max(Comparator.naturalOrder()).orElse(-1) + 1;
    RelCollation newCollation = _collation.apply(Mappings.source(mpListKeys, targetCount));
    Type newType = _type;
    if (newType == Type.HASH_PARTITIONED && newHashDesc.isEmpty()) {
      newType = Type.RANDOM;
    }
    return new PinotDataDistribution(newType, _workers, _workerHash, newHashDesc, newCollation);
  }

  @Nullable
  public HashDistributionDesc satisfiesHashDistributionConstraint(RelDistribution hashConstraint) {
    Preconditions.checkNotNull(_hashDistributionDesc, "Found hashDistributionDesc null in satisfies");
    /* TODO: once we add support for non-strict distributions, we can just check partial distributions.
    for (HashDistributionDesc desc : _hashDistributionDesc) {
      if (new HashSet<>(desc._keyIndexes).containsAll(hashConstraint.getKeys())) {
        return desc;
      }
    } */
    return _hashDistributionDesc.stream().filter(x -> x.getKeyIndexes().equals(hashConstraint.getKeys())).findFirst().orElse(null);
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

  private List<Integer> createMappingList(Map<Integer, Integer> mp) {
    List<Integer> result = new ArrayList<>();
    for (int i = 0; i < mp.size(); i++) {
      result.add(mp.get(i));
    }
    return result;
  }

  private void validate() {
    if (_type == Type.SINGLETON && _workers.size() > 1) {
      throw new IllegalStateException(String.format("Singleton distribution with %s workers", _workers.size()));
    }
  }
}
