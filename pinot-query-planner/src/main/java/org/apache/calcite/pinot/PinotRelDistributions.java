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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.commons.collections.MapUtils;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.TableConfig;


public class PinotRelDistributions {
  private static final String DEFAULT_HASH_FUNCTION = "murmur";

  private PinotRelDistributions() {
  }

  public static final PinotRelDistribution ANY = new PinotRelDistribution(
      Collections.emptyList(), null, null, RelDistribution.Type.ANY);

  public static final PinotRelDistribution SINGLETON = new PinotRelDistribution(
      Collections.emptyList(), null, null, RelDistribution.Type.SINGLETON);

  public static final PinotRelDistribution BROADCAST = new PinotRelDistribution(
      Collections.emptyList(), null, null, RelDistribution.Type.BROADCAST_DISTRIBUTED);

  public static final PinotRelDistribution RANDOM = new PinotRelDistribution(
      Collections.emptyList(), null, null, RelDistribution.Type.HASH_DISTRIBUTED);

  public static PinotRelDistribution hash(List<Integer> keys, Integer numPartitions) {
    return new PinotRelDistribution(keys, numPartitions, DEFAULT_HASH_FUNCTION, RelDistribution.Type.HASH_DISTRIBUTED);
  }

  public static PinotRelDistribution hash(String columnName, Integer numPartitions, RelDataType rowType) {
    int columnIndex = rowType.getFieldNames().indexOf(columnName);
    Preconditions.checkState(columnIndex >= 0);
    List<Integer> columnIndices = Collections.singletonList(columnIndex);
    return PinotRelDistributions.hash(columnIndices, numPartitions);
  }

  public static PinotRelDistribution of(TableConfig tableConfig, RelDataType rowType) {
    Map<String, ColumnPartitionConfig> partitionConfigMap = getColumnPartitionMap(tableConfig);
    if (partitionConfigMap.size() == 1) {
      String columnName = partitionConfigMap.keySet().iterator().next();
      ColumnPartitionConfig columnPartitionConfig = partitionConfigMap.values().iterator().next();
      Integer columnIndex = rowType.getFieldNames().indexOf(columnName);
      List<Integer> columnIndices = Collections.singletonList(columnIndex);
      return new PinotRelDistribution(columnIndices,
          columnPartitionConfig.getNumPartitions(), columnPartitionConfig.getFunctionName(), RelDistribution.Type.HASH_DISTRIBUTED);
    }
    return PinotRelDistributions.ANY;
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
