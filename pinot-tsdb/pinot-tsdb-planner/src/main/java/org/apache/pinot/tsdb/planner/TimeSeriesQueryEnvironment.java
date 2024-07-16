package org.apache.pinot.tsdb.planner;

import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.core.routing.RoutingManager;
import org.apache.pinot.tsdb.planner.physical.TimeSeriesDispatchablePlan;
import org.apache.pinot.tsdb.spi.RangeTimeSeriesRequest;
import org.apache.pinot.tsdb.spi.TimeSeriesLogicalPlanResult;


public class TimeSeriesQueryEnvironment {
  private final RoutingManager _routingManager;
  private final TableCache _tableCache;

  public TimeSeriesQueryEnvironment(RoutingManager routingManager, TableCache tableCache) {
    _routingManager = routingManager;
    _tableCache = tableCache;
  }

  public TimeSeriesLogicalPlanResult buildLogicalPlan(RangeTimeSeriesRequest request) {
    return null;
  }

  public TimeSeriesDispatchablePlan buildPhysicalPlan(TimeSeriesLogicalPlanResult logicalPlan) {
    return null;
  }
}
