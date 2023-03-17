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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelMultipleTrait;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.TableConfig;


public class PinotRelDistribution implements RelDistribution {
  @Nullable
  private final Set<List<Integer>> _colocationKeys;
  @Nullable
  private final Integer _numPartitions;
  @Nullable
  private final String _functionName;

  private PinotRelDistribution(Set<List<Integer>> colocationKeys, Integer numPartitions, String functionName) {
    _colocationKeys = colocationKeys;
    _numPartitions = numPartitions;
    _functionName = functionName;
  }

  public static PinotRelDistribution create(TableConfig tableConfig, CalciteSchema calciteSchema) {
    Map<String, ColumnPartitionConfig> partitionConfigMap = getColumnPartitionMap(tableConfig);
    if (partitionConfigMap.size() == 1) {
      String columnName = partitionConfigMap.keySet().iterator().next();
      ColumnPartitionConfig columnPartitionConfig = partitionConfigMap.values().iterator().next();
      Integer columnIndex = -1;
      List<Integer> columnIndices = Collections.singletonList(columnIndex);
      return new PinotRelDistribution(Set.of(columnIndices),
          columnPartitionConfig.getNumPartitions(), columnPartitionConfig.getFunctionName());
    }
    return new PinotRelDistribution(null, null, null);
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

  @Override
  public Type getType() {
    return null;
  }

  @Override
  public List<Integer> getKeys() {
    return CollectionUtils.isNotEmpty(_colocationKeys) && _colocationKeys.size() == 1
        ? _colocationKeys.iterator().next() : null;
  }

  @Override
  public RelDistribution apply(Mappings.TargetMapping mapping) {
    return null;
  }

  @Override
  public boolean isTop() {
    return false;
  }

  @Override
  public int compareTo(RelMultipleTrait o) {
    return 0;
  }

  @Override
  public RelTraitDef getTraitDef() {
    return null;
  }

  @Override
  public boolean satisfies(RelTrait trait) {
    return false;
  }

  @Override
  public void register(RelOptPlanner planner) {
  }

  public Set<List<Integer>> colocationKeys() {
    return _colocationKeys;
  }
}
