package org.apache.pinot.query.planner.logical.rel2plan;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.calcite.rel.PinotDataDistribution;
import org.apache.pinot.calcite.rel.hint.PinotHintOptions;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.DataSource;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.core.routing.RoutingManager;
import org.apache.pinot.core.routing.RoutingTable;
import org.apache.pinot.core.routing.TimeBoundaryInfo;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.query.context.PhysicalPlannerContext;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.routing.QueryServerInstance;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.sql.parsers.CalciteSqlCompiler;
import org.apache.pinot.sql.parsers.CalciteSqlParser;


public class LeafWorkerAssignment {
  private final PhysicalPlannerContext _physicalPlannerContext;
  private final RoutingManager _routingManager;

  public LeafWorkerAssignment(PhysicalPlannerContext physicalPlannerContext) {
    _physicalPlannerContext = physicalPlannerContext;
    _routingManager = physicalPlannerContext.getRoutingManager();
  }

  public PRelNode compute(PRelNode pRelNode, long requestId) {
    Context context = new Context();
    context._requestId = requestId;
    return assignWorkersInternal(pRelNode, context);
  }

  private PRelNode assignWorkersInternal(PRelNode pRelNode, Context context) {
    if (pRelNode.getRelNode() instanceof TableScan) {
      // TODO: For dim-tables, start with broadcast.
      return assignWorkersToLeafInternal(context, pRelNode);
    }
    List<PRelNode> newInputs = new ArrayList<>();
    for (PRelNode input : pRelNode.getInputs()) {
      newInputs.add(assignWorkersInternal(input, context));
    }
    PinotDataDistribution pinotDataDistribution = pRelNode.getPinotDataDistributionOrThrow();
    if (pRelNode.isLeafStage()) {
      Preconditions.checkState(pRelNode.getInputs().size() == 1,
          "Expected exactly 1 input in leaf stage nodes except table scan");
      Preconditions.checkState(pRelNode.getRelNode().getTraitSet().isEmpty() || pRelNode.isLeafStageBoundary(),
          "Leaf stage can only have traits on boundaries");
      PinotDataDistribution inputDistribution = pRelNode.getInputs().get(0).getPinotDataDistributionOrThrow();
      // compute mapping from source to destination.
      pinotDataDistribution = inputDistribution.apply(MappingGen.compute(
          pRelNode.getInputs().get(0).getRelNode(), pRelNode.getRelNode(), null));
    }
    return pRelNode.withNewInputs(pRelNode.getNodeId(), newInputs, pinotDataDistribution);
  }

  private PRelNode assignWorkersToLeafInternal(Context context, PRelNode pRelNode) {
    TableScan tableScan = (TableScan) pRelNode.getRelNode();
    String tableName = tableScan.getTable().getQualifiedName().get(1);
    // TODO: Support server pruning.
    String filter = "";
    Map<String, RoutingTable> routingTableMap = getRoutingTable(tableName, context._requestId);
    Preconditions.checkState(!routingTableMap.isEmpty(), "Unable to find routing entries for table: %s", tableName);

    // acquire time boundary info if it is a hybrid table.
    if (routingTableMap.size() > 1) {
      TimeBoundaryInfo timeBoundaryInfo = _routingManager.getTimeBoundaryInfo(
          TableNameBuilder.forType(TableType.OFFLINE)
              .tableNameWithType(TableNameBuilder.extractRawTableName(tableName)));
      if (timeBoundaryInfo != null) {
        _physicalPlannerContext.getTimeBoundaryInfoMap().put(pRelNode.getNodeId(), timeBoundaryInfo);
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
      if (!routingTable.getUnavailableSegments().isEmpty()) {
        // Set unavailable segments in context, keyed by PRelNode ID.
        _physicalPlannerContext.getUnavailableSegmentsMap()
            .computeIfAbsent(pRelNode.getNodeId(), (x) -> new HashMap<>())
            .put(tableName, new HashSet<>(routingTable.getUnavailableSegments()));
      }
    }
    int workerId = 0;
    Map<Integer, Map<String, List<String>>> workerIdToSegmentsMap = new HashMap<>();
    List<String> workers = new ArrayList<>();
    for (Map.Entry<ServerInstance, Map<String, List<String>>> entry : serverInstanceToSegmentsMap.entrySet()) {
      String instanceId = entry.getKey().getInstanceId();
      workerIdToSegmentsMap.put(workerId, entry.getValue());
      workers.add(String.format("%s@%s", workerId, instanceId));
      _physicalPlannerContext.getInstanceIdToQueryServerInstance().putIfAbsent(
          instanceId, new QueryServerInstance(entry.getKey()));
      workerId++;
    }
    _physicalPlannerContext.getWorkerIdToSegmentsMap().computeIfAbsent(
        pRelNode.getNodeId(), (x) -> new HashMap<>()).putAll(workerIdToSegmentsMap);
    List<RelHint> hints = ((TableScan) pRelNode.getRelNode()).getHints();
    _physicalPlannerContext.getScannedTableMap().computeIfAbsent(pRelNode.getNodeId(), (x) -> new HashSet<>())
        .add(tableName);
    _physicalPlannerContext.getTableOptionsMap().put(pRelNode.getNodeId(),
        PlanNode.NodeHint.fromRelHints(hints).getHintOptions().get(PinotHintOptions.TABLE_HINT_OPTIONS));
    PinotDataDistribution pinotDataDistribution = new PinotDataDistribution(PinotDataDistribution.Type.RANDOM,
        workers, workers.hashCode(), null, null);
    return pRelNode.withPinotDataDistribution(pinotDataDistribution);
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
