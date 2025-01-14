package org.apache.pinot.query.planner.logical.rel2plan;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.calcite.rel.PinotDataDistribution;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.DataSource;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.core.routing.RoutingManager;
import org.apache.pinot.core.routing.RoutingTable;
import org.apache.pinot.core.routing.TimeBoundaryInfo;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.query.routing.QueryServerInstance;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.sql.parsers.CalciteSqlCompiler;
import org.apache.pinot.sql.parsers.CalciteSqlParser;


public class LeafWorkerAssignment {
  private final RoutingManager _routingManager;

  public LeafWorkerAssignment(RoutingManager routingManager) {
    _routingManager = routingManager;
  }

  public Map<Integer, PinotDataDistribution> compute(RelNode relNode, long requestId) {
    Context context = new Context();
    context._requestId = requestId;
    assignWorkersToLeaf(relNode, context);
    return context._pinotDataDistributionMap;
  }

  private void assignWorkersToLeaf(RelNode relNode, Context context) {
    if (relNode instanceof TableScan) {
      assignWorkersToLeafInternal(context, (TableScan) relNode);
      return;
    }
    context._currentIndex++;
    context._parentNodeList.add(relNode);
    for (RelNode input : relNode.getInputs()) {
      assignWorkersToLeaf(input, context);
    }
    context._parentNodeList.remove(context._parentNodeList.size() - 1);
  }

  private void assignWorkersToLeafInternal(Context context, TableScan tableScan) {
    String tableName = tableScan.getTable().getQualifiedName().get(1);
    String filter = "";
    // TODO: Support server pruning.
    Map<String, RoutingTable> routingTableMap = getRoutingTable(tableName, context._requestId);
    Preconditions.checkState(!routingTableMap.isEmpty(), "Unable to find routing entries for table: %s", tableName);

    // acquire time boundary info if it is a hybrid table.
    if (routingTableMap.size() > 1) {
      TimeBoundaryInfo timeBoundaryInfo = _routingManager.getTimeBoundaryInfo(
          TableNameBuilder.forType(TableType.OFFLINE)
              .tableNameWithType(TableNameBuilder.extractRawTableName(tableName)));
      if (timeBoundaryInfo != null) {
        // TODO: Do something.
        // metadata.setTimeBoundaryInfo(timeBoundaryInfo);
      } else {
        // remove offline table routing if no time boundary info is acquired.
        routingTableMap.remove(TableType.OFFLINE.name());
      }
    }

    // extract all the instances associated to each table type
    Map<ServerInstance, Map<String, List<String>>> serverInstanceToSegmentsMap = new HashMap<>();
    for (Map.Entry<String, RoutingTable> routingEntry : routingTableMap.entrySet()) {
      String tableType = routingEntry.getKey();
      RoutingTable routingTable = routingEntry.getValue();
      // for each server instance, attach all table types and their associated segment list.
      Map<ServerInstance, Pair<List<String>, List<String>>> segmentsMap = routingTable.getServerInstanceToSegmentsMap();
      for (Map.Entry<ServerInstance, Pair<List<String>, List<String>>> serverEntry : segmentsMap.entrySet()) {
        Map<String, List<String>> tableTypeToSegmentListMap =
            serverInstanceToSegmentsMap.computeIfAbsent(serverEntry.getKey(), k -> new HashMap<>());
        // TODO: support optional segments for multi-stage engine.
        Preconditions.checkState(tableTypeToSegmentListMap.put(tableType, serverEntry.getValue().getLeft()) == null,
            "Entry for server {} and table type: {} already exist!", serverEntry.getKey(), tableType);
      }

      // attach unavailable segments to metadata
      if (!routingTable.getUnavailableSegments().isEmpty()) {
        // TODO: Do something.
        // metadata.addUnavailableSegments(tableName, routingTable.getUnavailableSegments());
      }
    }
    int workerId = 0;
    Map<Integer, QueryServerInstance> workerIdToServerInstanceMap = new HashMap<>();
    Map<Integer, Map<String, List<String>>> workerIdToSegmentsMap = new HashMap<>();
    List<String> workers = new ArrayList<>();
    for (Map.Entry<ServerInstance, Map<String, List<String>>> entry : serverInstanceToSegmentsMap.entrySet()) {
      workerIdToServerInstanceMap.put(workerId, new QueryServerInstance(entry.getKey()));
      workerIdToSegmentsMap.put(workerId, entry.getValue());
      workers.add(String.format("%s@%s", workerId, entry.getKey().getInstanceId()));
      workerId++;
    }
    PinotDataDistribution pinotDataDistribution = new PinotDataDistribution(PinotDataDistribution.Type.RANDOM,
        workers, workers.hashCode(), null);
    context._pinotDataDistributionMap.put(context._currentIndex, pinotDataDistribution);
  }

  /**
   * Acquire routing table for items listed in TableScanNode.
   *
   * @param tableName table name with or without type suffix.
   * @return keyed-map from table type(s) to routing table(s).
   */
  private Map<String, RoutingTable> getRoutingTable(String tableName, long requestId) {
    String rawTableName = TableNameBuilder.extractRawTableName(tableName);
    TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableName);
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
      routingTable = getRoutingTable(tableName, tableType, requestId);
      if (routingTable != null) {
        routingTableMap.put(tableType.name(), routingTable);
      }
    }
    return routingTableMap;
  }

  private RoutingTable getRoutingTable(String tableName, TableType tableType, long requestId) {
    String tableNameWithType =
        TableNameBuilder.forType(tableType).tableNameWithType(TableNameBuilder.extractRawTableName(tableName));
    return _routingManager.getRoutingTable(
        CalciteSqlCompiler.compileToBrokerRequest("SELECT * FROM \"" + tableNameWithType + "\""), requestId);
  }

  public static class Context {
    long _requestId = 0;
    int _currentIndex = 0;
    List<RelNode> _parentNodeList = new ArrayList<>(32);
    Map<Integer, PinotDataDistribution> _pinotDataDistributionMap = new HashMap<>();
  }

  private static BrokerRequest createBrokerRequest(String tableName, String filter) {
    BrokerRequest brokerRequest = new BrokerRequest();
    PinotQuery pinotQuery = new PinotQuery();
    DataSource dataSource = new DataSource();
    dataSource.setTableName(tableName);
    pinotQuery.setDataSource(dataSource);
    pinotQuery.setFilterExpression(CalciteSqlParser.compileToExpression(filter));
    brokerRequest.setPinotQuery(pinotQuery);
    return brokerRequest;
  }
}
