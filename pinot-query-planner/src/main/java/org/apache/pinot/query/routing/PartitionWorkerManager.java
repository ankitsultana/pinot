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
package org.apache.pinot.query.routing;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.collections.MapUtils;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.core.routing.RoutingManager;
import org.apache.pinot.core.routing.RoutingTable;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.query.planner.PlannerUtils;
import org.apache.pinot.query.planner.QueryPlan;
import org.apache.pinot.query.planner.StageMetadata;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.sql.parsers.CalciteSqlCompiler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PartitionWorkerManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(PartitionWorkerManager.class);
  private String _hostName;
  private int _port;
  private RoutingManager _routingManager;
  private Map<Integer, StageMetadata> _stageMetadataMap;
  private Map<Integer, List<Integer>> _stageTree;
  private long _requestId;
  private Map<String, String> _options;
  private TableCache _tableCache;
  private Set<Integer> _leafStagesWithLocalizedPartitions = new HashSet<>();

  public PartitionWorkerManager(String hostName, int port, RoutingManager routingManager, QueryPlan queryPlan,
      Map<Integer, List<Integer>> stageTree, long requestId, Map<String, String> options, TableCache tableCache) {
    _hostName = hostName;
    _port = port;
    _routingManager = routingManager;
    _stageMetadataMap = queryPlan.getStageMetadataMap();
    _stageTree = stageTree;
    _requestId = requestId;
    _options = options;
    _tableCache = tableCache;
  }

  public void assign(int stageId) {
    StageMetadata stageMetadata = _stageMetadataMap.get(stageId);
    if (PlannerUtils.isRootStage(stageId)) {
      stageMetadata.setVirtualServers(Lists.newArrayList(
          new VirtualServer(new WorkerInstance(_hostName, _port, _port, _port, _port), 0)));
      for (Integer inputStage : _stageTree.get(stageId)) {
        assign(inputStage);
      }
      return;
    }
    List<String> scannedTables = stageMetadata.getScannedTables();
    if (scannedTables.size() == 1) {
      String tableName = scannedTables.get(0);
      TableConfig tableConfig = _tableCache.getTableConfig(tableName);
      Preconditions.checkNotNull(tableConfig, "Use table name with table-type");
      int numPartitions = -1;
      if (tableConfig.getIndexingConfig().getSegmentPartitionConfig() != null) {
        SegmentPartitionConfig segmentPartitionConfig = tableConfig.getIndexingConfig().getSegmentPartitionConfig();
        if (MapUtils.isNotEmpty(segmentPartitionConfig.getColumnPartitionMap())
            && segmentPartitionConfig.getColumnPartitionMap().size() == 1) {
          numPartitions = segmentPartitionConfig.getColumnPartitionMap().values().iterator().next().getNumPartitions();
        }
      }
      int stageParallelism = getStageParallelism(_options);
      if (numPartitions == -1) {
        LOGGER.info("No shard column found. Spraying segments across query partitions");
        spraySegmentsAcrossQueryPartitions(stageMetadata, stageParallelism);
        return;
      } else if (stageParallelism != -1 && stageParallelism != numPartitions) {
        LOGGER.info("Shard column found but numPartitions={} not equal to parallelism={}",
            numPartitions, stageParallelism);
        spraySegmentsAcrossQueryPartitions(stageMetadata, stageParallelism);
        return;
      }
      try {
        if (assignLeafStageForPartitionedTable(stageMetadata, stageParallelism)) {
          _leafStagesWithLocalizedPartitions.add(stageId);
        }
      } catch (Exception e) {
        LOGGER.info("Spraying");
        spraySegmentsAcrossQueryPartitions(stageMetadata, stageParallelism);
      }
      return;
    }
    List<Integer> childStages = _stageTree.get(stageId);
    assign(childStages.get(0));
    List<VirtualServer> toStageServers = _stageMetadataMap.get(childStages.get(0)).getVirtualServers();
    if (childStages.size() == 2) {
      assign(childStages.get(1));
      List<VirtualServer> otherDepServers = _stageMetadataMap.get(childStages.get(1)).getVirtualServers();
      if (otherDepServers.size() > toStageServers.size()) {
        toStageServers = otherDepServers;
      }
    }
    if (stageMetadata.isRequiresSingletonInstance()) {
      _stageMetadataMap.get(stageId).setVirtualServers(Collections.singletonList(toStageServers.get(0)));
    } else {
      _stageMetadataMap.get(stageId).setVirtualServers(new ArrayList<>(toStageServers));
    }
  }

  public Set<Integer> getLeafStagesWithLocalizedPartitions() {
    return _leafStagesWithLocalizedPartitions;
  }

  private boolean assignLeafStageForPartitionedTable(StageMetadata stageMetadata, int stageParallelism) {
    List<String> scannedTables = stageMetadata.getScannedTables();
    String logicalTableName = scannedTables.get(0);
    Map<String, RoutingTable> routingTableMap = getRoutingTable(logicalTableName, _requestId);
    Preconditions.checkState(routingTableMap.size() == 1,
        String.format("Found %s entries in routing-table", routingTableMap.size()));
    RoutingTable routingTable = routingTableMap.values().iterator().next();
    String tableType = routingTableMap.keySet().iterator().next();
    Map<String, ServerInstance> segmentToServerMap = routingTable.getSegmentToServerMap();
    int numServers = new HashSet<>(segmentToServerMap.values()).size();
    Map<Integer, ServerInstance> partitionIdToServer = new HashMap<>();
    boolean arePartitionsLocalized = true;
    for (Map.Entry<String, Integer> entry : routingTable.getSegmentToPartitionMap().entrySet()) {
      String segmentName = entry.getKey();
      Integer partitionId = entry.getValue();
      if (partitionId == -1) {
        arePartitionsLocalized = false;
        break;
      }
      if (partitionIdToServer.containsKey(partitionId)) {
        if (!partitionIdToServer.get(partitionId).equals(segmentToServerMap.get(segmentName))) {
          arePartitionsLocalized = false;
          break;
        }
      } else {
        partitionIdToServer.put(partitionId, segmentToServerMap.get(segmentName));
      }
    }
    if (!arePartitionsLocalized) {
      throw new RuntimeException("Partitions are not localized");
    }
    if (stageParallelism == -1) {
      Map<ServerInstance, Integer> serverResidue = new HashMap<>();
      for (Map.Entry<Integer, ServerInstance> entry : partitionIdToServer.entrySet()) {
        int partitionId = entry.getKey();
        ServerInstance serverInstance = entry.getValue();
        int residue = partitionId % numServers;
        if (serverResidue.containsKey(serverInstance)) {
          if (serverResidue.get(serverInstance) != residue) {
            throw new RuntimeException(
                "Partitions not localized because partitions with same residue on different servers");
          }
        } else {
          serverResidue.put(serverInstance, residue);
        }
      }
      Preconditions.checkState(serverResidue.size() == numServers,
          "[likely code-bug] partitionId mod numServers doesn't have numServers unique values");
      List<VirtualServer> virtualServers = new ArrayList<>(numServers);
      for (int i = 0; i < numServers; i++) {
        virtualServers.add(null);
      }
      Map<ServerInstance, VirtualServer> serverToVirtualServer = new HashMap<>();
      for (Map.Entry<ServerInstance, Integer> entry : serverResidue.entrySet()) {
        ServerInstance serverInstance = entry.getKey();
        int virtualId = entry.getValue();
        Preconditions.checkState(virtualServers.get(virtualId) == null,
            "[likely code-bug] two servers with same residue");
        VirtualServer virtualServer = new VirtualServer(serverInstance, virtualId);
        virtualServers.set(virtualId, virtualServer);
        serverToVirtualServer.put(serverInstance, virtualServer);
      }
      Map<VirtualServer, Map<String, List<String>>> virtualServerToSegmentsMap = new HashMap<>();
      for (Map.Entry<String, ServerInstance> entry : segmentToServerMap.entrySet()) {
        String segment = entry.getKey();
        ServerInstance serverInstance = entry.getValue();
        VirtualServer virtualServer = serverToVirtualServer.get(serverInstance);
        virtualServerToSegmentsMap.computeIfAbsent(virtualServer, (x) -> new HashMap<>());
        virtualServerToSegmentsMap.get(virtualServer).computeIfAbsent(tableType, (x) -> new ArrayList<>());
        virtualServerToSegmentsMap.get(virtualServer).get(tableType).add(segment);
      }
      stageMetadata.setVirtualServers(virtualServers);
      stageMetadata.setVirtualServerToSegmentsMap(virtualServerToSegmentsMap);
      return true;
    }
    List<VirtualServer> virtualServers = new ArrayList<>(stageParallelism);
    for (int virtualId = 0; virtualId < stageParallelism; virtualId++) {
      virtualServers.add(new VirtualServer(partitionIdToServer.get(virtualId), virtualId));
    }
    Map<VirtualServer, Map<String, List<String>>> virtualServerToSegmentsMap = new HashMap<>();
    for (Map.Entry<String, Integer> entry : routingTable.getSegmentToPartitionMap().entrySet()) {
      String segmentName = entry.getKey();
      Integer partition = entry.getValue();
      VirtualServer virtualServer = virtualServers.get(partition);
      virtualServerToSegmentsMap.computeIfAbsent(virtualServer, (x) -> new HashMap<>());
      virtualServerToSegmentsMap.get(virtualServer).computeIfAbsent(tableType, (x) -> new ArrayList<>());
      virtualServerToSegmentsMap.get(virtualServer).get(tableType).add(segmentName);
    }
    stageMetadata.setVirtualServers(virtualServers);
    stageMetadata.setVirtualServerToSegmentsMap(virtualServerToSegmentsMap);
    return true;
  }

  private void spraySegmentsAcrossQueryPartitions(StageMetadata stageMetadata, int stageParallelism) {
    List<String> scannedTables = stageMetadata.getScannedTables();
    String logicalTableName = scannedTables.get(0);
    Map<String, RoutingTable> routingTableMap = getRoutingTable(logicalTableName, _requestId);
    Preconditions.checkState(routingTableMap.size() == 1,
        String.format("Found %s entries in routing-table", routingTableMap.size()));
    RoutingTable routingTable = routingTableMap.values().iterator().next();
    String tableType = routingTableMap.keySet().iterator().next();
    Map<String, ServerInstance> segmentToServerMap = routingTable.getSegmentToServerMap();
    int numServers = segmentToServerMap.values().size();
    if (stageParallelism == -1) {
      stageParallelism = numServers;
    }
    List<ServerInstance> servers = new ArrayList<>(new HashSet<>(segmentToServerMap.values()));
    Map<ServerInstance, List<VirtualServer>> serverToVirtualServers = new HashMap<>();
    Map<ServerInstance, AtomicInteger> rotatorByServer = new HashMap<>();
    for (ServerInstance serverInstance : servers) {
      serverToVirtualServers.put(serverInstance, new ArrayList<>());
      rotatorByServer.put(serverInstance, new AtomicInteger(0));
    }
    List<VirtualServer> virtualServers = new ArrayList<>();
    for (int virtualId = 0; virtualId < stageParallelism; virtualId++) {
      VirtualServer virtualServer = new VirtualServer(servers.get(virtualId % servers.size()), virtualId);
      virtualServers.add(virtualServer);
      serverToVirtualServers.get(virtualServer.getServer()).add(virtualServer);
    }
    Map<VirtualServer, Map<String, List<String>>> virtualServerToSegments = new HashMap<>();
    for (Map.Entry<String, ServerInstance> entry : segmentToServerMap.entrySet()) {
      String segment = entry.getKey();
      ServerInstance serverInstance = entry.getValue();
      int idx = rotatorByServer.get(serverInstance).getAndIncrement()
          % serverToVirtualServers.get(serverInstance).size();
      VirtualServer virtualServer = serverToVirtualServers.get(serverInstance).get(idx);
      virtualServerToSegments.computeIfAbsent(virtualServer, (x) -> new HashMap<>());
      virtualServerToSegments.get(virtualServer).computeIfAbsent(tableType, (x) -> new ArrayList<>());
      virtualServerToSegments.get(virtualServer).get(tableType).add(segment);
    }
    stageMetadata.setVirtualServers(virtualServers);
    stageMetadata.setVirtualServerToSegmentsMap(virtualServerToSegments);
  }

  private Map<String, RoutingTable> getRoutingTable(String logicalTableName, long requestId) {
    String rawTableName = TableNameBuilder.extractRawTableName(logicalTableName);
    TableType tableType = TableNameBuilder.getTableTypeFromTableName(logicalTableName);
    Map<String, RoutingTable> routingTableMap = new HashMap<>();
    RoutingTable routingTable;
    if (tableType == null) {
      routingTable = getRoutingTable(rawTableName, TableType.OFFLINE, requestId);
      if (routingTable != null) {
        routingTableMap.put(TableType.OFFLINE.name(), routingTable);
      }
      routingTable = getRoutingTable(rawTableName, TableType.REALTIME, requestId);
      if (routingTable != null) {
        routingTableMap.put(TableType.REALTIME.name(), routingTable);
      }
    } else {
      routingTable = getRoutingTable(logicalTableName, tableType, requestId);
      if (routingTable != null) {
        routingTableMap.put(tableType.name(), routingTable);
      }
    }
    return routingTableMap;
  }

  static int getStageParallelism(Map<String, String> options) {
    return Integer.parseInt(
        options.getOrDefault(CommonConstants.Broker.Request.QueryOptionKey.STAGE_PARALLELISM, "-1"));
  }

  private RoutingTable getRoutingTable(String tableName, TableType tableType, long requestId) {
    String tableNameWithType = TableNameBuilder.forType(tableType).tableNameWithType(
        TableNameBuilder.extractRawTableName(tableName));
    return _routingManager.getRoutingTable(
        CalciteSqlCompiler.compileToBrokerRequest("SELECT * FROM " + tableNameWithType), requestId);
  }
}
