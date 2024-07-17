package org.apache.pinot.tsdb.planner;

import com.google.common.base.Preconditions;
import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.DataSource;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.request.QuerySource;
import org.apache.pinot.core.routing.RoutingManager;
import org.apache.pinot.core.routing.RoutingTable;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.trace.RequestContext;
import org.apache.pinot.tsdb.planner.physical.TimeSeriesDispatchablePlan;
import org.apache.pinot.tsdb.planner.physical.TimeSeriesQueryServerInstance;
import org.apache.pinot.tsdb.planner.physical.serde.TimeSeriesPlanSerde;
import org.apache.pinot.tsdb.spi.PinotTimeSeriesConfigs;
import org.apache.pinot.tsdb.spi.RangeTimeSeriesRequest;
import org.apache.pinot.tsdb.spi.TimeSeriesLogicalPlanResult;
import org.apache.pinot.tsdb.spi.TimeSeriesLogicalPlanner;
import org.apache.pinot.tsdb.spi.plan.BaseTimeSeriesPlanNode;
import org.apache.pinot.tsdb.spi.plan.ScanFilterAndProjectPlanNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TimeSeriesQueryEnvironment {
  private static final Logger LOGGER = LoggerFactory.getLogger(TimeSeriesQueryEnvironment.class);
  private final RoutingManager _routingManager;
  private final TableCache _tableCache;
  private final Map<String, TimeSeriesLogicalPlanner> _plannerMap = new HashMap<>();

  public TimeSeriesQueryEnvironment(PinotConfiguration config, RoutingManager routingManager, TableCache tableCache) {
    _routingManager = routingManager;
    _tableCache = tableCache;
  }

  public void init(PinotConfiguration config) {
    String[] engines = config.getProperty(PinotTimeSeriesConfigs.CommonConfigs.TIME_SERIES_ENGINES, "")
        .split(",");
    LOGGER.info("Found {} configured time series engines. List: {}", engines.length, engines);
    for (String engine : engines) {
      String configPrefix = PinotTimeSeriesConfigs.TIME_SERIES_ENGINE_CONFIG_PREFIX + "." + engine;
      String klassName = config.getProperty(
          configPrefix + "." + PinotTimeSeriesConfigs.BrokerConfigs.LOGICAL_PLANNER_CLASS_SUFFIX);
      Preconditions.checkNotNull(klassName, "Logical planner class not found for engine: " + engine);
      // Create the planner with empty constructor
      try {
        Class<?> klass = Class.forName(klassName);
        Constructor<?> constructor = klass.getConstructor();
        TimeSeriesLogicalPlanner planner = (TimeSeriesLogicalPlanner) constructor.newInstance();
        planner.init(config.subset(configPrefix).toMap());
        _plannerMap.put(engine, planner);
      } catch (Exception e) {
        throw new RuntimeException("Failed to instantiate logical planner for engine: " + engine, e);
      }
    }
  }

  public TimeSeriesLogicalPlanResult buildLogicalPlan(RangeTimeSeriesRequest request) {
    // TODO: Add validation and some other steps here.
    Preconditions.checkState(_plannerMap.containsKey(request.getEngine()),
        "No logical planner found for engine: %s. Available: %s", request.getEngine(),
        _plannerMap.keySet());
    return _plannerMap.get(request.getEngine()).plan(request);
  }

  public TimeSeriesDispatchablePlan buildPhysicalPlan(
      RequestContext requestContext, TimeSeriesLogicalPlanResult logicalPlan) {
    final Set<String> tableNames = new HashSet<>();
    findTableNames(logicalPlan.getPlanNode(), tableNames::add);
    Preconditions.checkState(tableNames.size() == 1,
        "Expected exactly one table name in the logical plan, got: %s",
        tableNames);
    String tableName = tableNames.iterator().next();
    RoutingTable routingTable = _routingManager.getRoutingTable(compileBrokerRequest(tableName),
        requestContext.getRequestId());
    Preconditions.checkState(routingTable != null,
        "Failed to get routing table for table: %s", tableName);
    Preconditions.checkState(routingTable.getServerInstanceToSegmentsMap().size() == 1,
        "Expected exactly one server instance in the routing table, got: %s",
        routingTable.getServerInstanceToSegmentsMap().size());
    var entry = routingTable.getServerInstanceToSegmentsMap().entrySet().iterator().next();
    ServerInstance serverInstance = entry.getKey();
    List<String> segments =  entry.getValue().getLeft();
    return new TimeSeriesDispatchablePlan(new TimeSeriesQueryServerInstance(serverInstance),
        TimeSeriesPlanSerde.serialize(logicalPlan.getPlanNode()), segments);
  }

  public static void findTableNames(BaseTimeSeriesPlanNode planNode, Consumer<String> tableNameConsumer) {
    if (planNode instanceof ScanFilterAndProjectPlanNode) {
      ScanFilterAndProjectPlanNode scanNode = (ScanFilterAndProjectPlanNode) planNode;
      tableNameConsumer.accept(scanNode.getTableName());
      return;
    }
    for (BaseTimeSeriesPlanNode childNode : planNode.getChildren()) {
      findTableNames(childNode, tableNameConsumer);
    }
  }

  private BrokerRequest compileBrokerRequest(String tableName) {
    DataSource dataSource = new DataSource();
    dataSource.setTableName(tableName);
    PinotQuery pinotQuery = new PinotQuery();
    pinotQuery.setDataSource(dataSource);
    QuerySource querySource = new QuerySource();
    querySource.setTableName(tableName);
    BrokerRequest dummyRequest = new BrokerRequest();
    dummyRequest.setPinotQuery(pinotQuery);
    dummyRequest.setQuerySource(querySource);
    return dummyRequest;
  }
}
