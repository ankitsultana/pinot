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

import java.util.Map;
import org.apache.calcite.plan.Context;
import org.apache.commons.collections.MapUtils;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.checkerframework.checker.nullness.qual.Nullable;


public class PinotPlannerSessionContext implements Context {
  public static final String COLOCATED_JOIN_FLAG = "useColocatedJoin";
  public static final String PARALLELISM_OPTION = "queryParallelism";
  private final TableCache _tableCache;
  private final boolean _isColocatedJoin;
  private final Integer _queryParallelism;

  private PinotPlannerSessionContext(Map<String, String> options, TableCache tableCache) {
    _tableCache = tableCache;
    _queryParallelism = Integer.parseInt(options.getOrDefault(PARALLELISM_OPTION, "-1"));
    _isColocatedJoin = options.getOrDefault(COLOCATED_JOIN_FLAG, "false").equals("true");
  }

  public Integer getQueryParallelism() {
    return _queryParallelism;
  }

  public boolean isColocationEnabled() {
    return _isColocatedJoin;
  }

  @Nullable
  public TableConfig getTableConfig(String tableName) {
    String rawTableName = TableNameBuilder.extractRawTableName(tableName);
    TableConfig result = _tableCache.getTableConfig(TableNameBuilder.OFFLINE.tableNameWithType(rawTableName));
    if (result == null) {
      return _tableCache.getTableConfig(TableNameBuilder.REALTIME.tableNameWithType(rawTableName));
    }
    return result;
  }

  public static PinotPlannerSessionContext of(Map<String, String> queryOptions, TableCache tableCache) {
    return new PinotPlannerSessionContext(queryOptions, tableCache);
  }

  @Nullable
  public static Map<String, ColumnPartitionConfig> getTablePartitionConfigSafe(TableConfig tableConfig) {
    if (tableConfig.getIndexingConfig() != null) {
      SegmentPartitionConfig segmentPartitionConfig = tableConfig.getIndexingConfig().getSegmentPartitionConfig();
      if (segmentPartitionConfig != null) {
        Map<String, ColumnPartitionConfig> columnPartitionConfigMap = segmentPartitionConfig.getColumnPartitionMap();
        if (MapUtils.isNotEmpty(columnPartitionConfigMap) && columnPartitionConfigMap.size() == 1) {
          return columnPartitionConfigMap;
        }
      }
    }
    return null;
  }

  @Override
  public <C> @Nullable C unwrap(Class<C> aClass) {
    return null;
  }
}
