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

import com.google.common.collect.Ordering;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.calcite.plan.RelMultipleTrait;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.util.mapping.Mapping;
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
    List<Integer> mappedKeys = Mappings.apply2((Mapping) mapping, _keys);
    // TODO: Some canonicalization is required perhaps
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
    throw new UnsupportedOperationException("satisfies is not implemented");
    /*
    Preconditions.checkState(trait instanceof PinotRelDistribution);
    PinotRelDistribution inputTrait = (PinotRelDistribution) trait;
    if (_type.equals(RelDistribution.Type.ANY)) {
      return true;
    } else if (_type.equals(Type.HASH_DISTRIBUTED)) {
      if (inputTrait.getType().equals(Type.HASH_DISTRIBUTED)) {
        Preconditions.checkState(_numPartitions != null && _functionName != null);
        return _keys.containsAll(inputTrait.getKeys())
            && _numPartitions.equals(inputTrait._numPartitions)
            && _functionName.equals(inputTrait._functionName);
      }
      return false;
    } else if (_type.equals(Type.BROADCAST_DISTRIBUTED)) {
    }
    return false; */
  }

  @Override
  public void register(RelOptPlanner planner) {
  }

  public static PinotRelDistribution create(TableConfig tableConfig) {
    Map<String, ColumnPartitionConfig> partitionConfigMap = getColumnPartitionMap(tableConfig);
    if (partitionConfigMap.size() == 1) {
      String columnName = partitionConfigMap.keySet().iterator().next();
      ColumnPartitionConfig columnPartitionConfig = partitionConfigMap.values().iterator().next();
      // TODO: How to determine columnIndex from schema
      Integer columnIndex = -1;
      List<Integer> columnIndices = Collections.singletonList(columnIndex);
      return new PinotRelDistribution(columnIndices,
          columnPartitionConfig.getNumPartitions(), columnPartitionConfig.getFunctionName(), Type.HASH_DISTRIBUTED);
    }
    return new PinotRelDistribution(Collections.emptyList(), null, null, Type.ANY);
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
