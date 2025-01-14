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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;


public class PinotDataDistribution {
  private final Type _type;
  private final List<String> _workers;
  private final long _workerHash;
  @Nullable
  private final Set<HashDistributionDesc> _hashDistributionDesc;

  public PinotDataDistribution(Type type, List<String> workers, long workerHash,
      @Nullable Set<HashDistributionDesc> desc) {
    _type = type;
    _workers = workers;
    _workerHash = workerHash;
    _hashDistributionDesc = desc;
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

  /**
   * <pre>
   *   Input: distribution required logically for correctness.
   *   Output: whether this physical distribution meets the input constraint.
   * </pre>
   */
  public boolean satisfies(RelDistribution distributionConstraint) {
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

  public boolean satisfies(RelDistribution... distributionConstraints) {
    for (RelDistribution distributionConstraint : distributionConstraints) {
      if (!satisfies(distributionConstraint)) {
        return false;
      }
    }
    return true;
  }

  @Nullable
  public HashDistributionDesc satisfiesHashDistributionConstraint(RelDistribution hashConstraint) {
    Preconditions.checkNotNull(_hashDistributionDesc, "Found hashDistributionDesc null in satisfies");
    // Calcite considers ordering of keys un-important. But now we only do ordered comparison.
    for (HashDistributionDesc desc : _hashDistributionDesc) {
      if (new HashSet<>(desc._keyIndexes).containsAll(hashConstraint.getKeys())) {
        return desc;
      }
    }
    return null;
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
  }

  private void validate() {
    if (_type == Type.SINGLETON && _workers.size() > 1) {
      throw new IllegalStateException(String.format("Singleton distribution with %s workers", _workers.size()));
    }
  }
}
