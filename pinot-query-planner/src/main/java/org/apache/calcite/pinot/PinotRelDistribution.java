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
import com.google.common.collect.Ordering;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.calcite.plan.RelMultipleTrait;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.commons.collections.MapUtils;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.TableConfig;


public class PinotRelDistribution implements RelDistribution {
  private static final Ordering<Iterable<Integer>> ORDERING =
      Ordering.<Integer>natural().lexicographical();

  public static final PinotRelDistribution ANY = new PinotRelDistribution(
      Collections.emptyList(), null, null, Type.ANY);

  private final List<Integer> _keys;
  @Nullable
  private final Integer _numPartitions;
  @Nullable
  private final String _functionName;
  private final Type _type;

  PinotRelDistribution(List<Integer> keys, Integer numPartitions, String functionName, Type type) {
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
  public RelDistribution apply(Mappings.TargetMapping mapping) {
    if (_keys.isEmpty()) {
      return this;
    }
    for (int key : _keys) {
      if (mapping.getTargetOpt(key) == -1) {
        return ANY;
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
      return String.format("hash=(%s)", _keys);
    } else if (_type.equals(Type.BROADCAST_DISTRIBUTED)) {
      return "broadcast";
    } else if (_type.equals(Type.ANY)) {
      return "any";
    } else if (_type.equals(Type.RANDOM_DISTRIBUTED)) {
      return "random";
    }
    return "unknown";
  }

  public static PinotRelDistribution create(TableConfig tableConfig, RelDataType rowType) {
    Map<String, ColumnPartitionConfig> partitionConfigMap = getColumnPartitionMap(tableConfig);
    if (partitionConfigMap.size() == 1) {
      String columnName = partitionConfigMap.keySet().iterator().next();
      ColumnPartitionConfig columnPartitionConfig = partitionConfigMap.values().iterator().next();
      Integer columnIndex = rowType.getFieldNames().indexOf(columnName);
      List<Integer> columnIndices = Collections.singletonList(columnIndex);
      return new PinotRelDistribution(columnIndices,
          columnPartitionConfig.getNumPartitions(), columnPartitionConfig.getFunctionName(), Type.HASH_DISTRIBUTED);
    }
    return PinotRelDistributions.ANY;
  }

  public static PinotRelDistribution create(String columnName, RelDataType rowType) {
    Integer columnIndex = rowType.getFieldNames().indexOf(columnName);
    Preconditions.checkState(columnIndex >= 0);
    List<Integer> columnIndices = Collections.singletonList(columnIndex);
    return PinotRelDistributions.hash(columnIndices, -1);
  }

  public static PinotRelDistribution of(RelDistribution relDistribution) {
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

  private static Map<String, ColumnPartitionConfig> getColumnPartitionMap(TableConfig tableConfig) {
    if (tableConfig.getIndexingConfig() != null) {
      if (tableConfig.getIndexingConfig().getSegmentPartitionConfig() != null) {
        SegmentPartitionConfig segmentPartitionConfig = tableConfig.getIndexingConfig().getSegmentPartitionConfig();
        if (MapUtils.isNotEmpty(segmentPartitionConfig.getColumnPartitionMap())) {
          return segmentPartitionConfig.getColumnPartitionMap();
        }
      }
    }
    return Collections.emptyMap();
  }
}
