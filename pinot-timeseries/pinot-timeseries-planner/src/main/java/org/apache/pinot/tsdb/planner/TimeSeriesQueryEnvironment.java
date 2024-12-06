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
package org.apache.pinot.tsdb.planner;

import com.google.common.base.Preconditions;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.core.routing.RoutingManager;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.trace.RequestContext;
import org.apache.pinot.tsdb.planner.physical.TableScanVisitor;
import org.apache.pinot.tsdb.planner.physical.TimeSeriesDispatchablePlan;
import org.apache.pinot.tsdb.planner.physical.TimeSeriesQueryServerInstance;
import org.apache.pinot.tsdb.spi.PinotTimeSeriesConfiguration;
import org.apache.pinot.tsdb.spi.RangeTimeSeriesRequest;
import org.apache.pinot.tsdb.spi.TimeSeriesLogicalPlanResult;
import org.apache.pinot.tsdb.spi.TimeSeriesLogicalPlanner;
import org.apache.pinot.tsdb.spi.plan.BaseTimeSeriesPlanNode;
import org.apache.pinot.tsdb.spi.plan.serde.TimeSeriesPlanSerde;
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
    String[] languages = config.getProperty(PinotTimeSeriesConfiguration.getEnabledLanguagesConfigKey(), "")
        .split(",");
    LOGGER.info("Found {} configured time series languages. List: {}", languages.length, languages);
    for (String language : languages) {
      String configPrefix = PinotTimeSeriesConfiguration.getLogicalPlannerConfigKey(language);
      String klassName =
          config.getProperty(PinotTimeSeriesConfiguration.getLogicalPlannerConfigKey(language));
      Preconditions.checkNotNull(klassName, "Logical planner class not found for language: " + language);
      // Create the planner with empty constructor
      try {
        Class<?> klass = TimeSeriesQueryEnvironment.class.getClassLoader().loadClass(klassName);
        Constructor<?> constructor = klass.getConstructor();
        TimeSeriesLogicalPlanner planner = (TimeSeriesLogicalPlanner) constructor.newInstance();
        planner.init(config.subset(configPrefix));
        _plannerMap.put(language, planner);
      } catch (Exception e) {
        throw new RuntimeException("Failed to instantiate logical planner for language: " + language, e);
      }
    }
    TableScanVisitor.INSTANCE.init(_routingManager);
  }

  public TimeSeriesLogicalPlanResult buildLogicalPlan(RangeTimeSeriesRequest request) {
    Preconditions.checkState(_plannerMap.containsKey(request.getLanguage()),
        "No logical planner found for engine: %s. Available: %s", request.getLanguage(),
        _plannerMap.keySet());
    return _plannerMap.get(request.getLanguage()).plan(request);
  }

  public TimeSeriesDispatchablePlan buildPhysicalPlan(RangeTimeSeriesRequest timeSeriesRequest,
      RequestContext requestContext, TimeSeriesLogicalPlanResult logicalPlan) {
    // Step-1: Assign segments to servers for each leaf node.
    TableScanVisitor.Context scanVisitorContext = TableScanVisitor.createContext(requestContext.getRequestId());
    TableScanVisitor.INSTANCE.assignSegmentsToPlan(logicalPlan.getPlanNode(), logicalPlan.getTimeBuckets(),
        scanVisitorContext);
    Map<String, Map<String, List<String>>> serverToSegmentsByPlanId =
        scanVisitorContext.computeServerToSegmentsByPlanId();
    List<TimeSeriesQueryServerInstance> serverInstances = scanVisitorContext.getPlanIdToSegmentsByServer().keySet()
        .stream().map(TimeSeriesQueryServerInstance::new).collect(Collectors.toList());
    // Step-2: Create plan fragments and serialize them.
    List<BaseTimeSeriesPlanNode> fragments = TimeSeriesPlanFragmenter.getFragments(
        logicalPlan.getPlanNode(), serverInstances.size() == 1);
    List<Pair<String, String>> serializedPlanFragments = new ArrayList<>();
    for (int index = 1; index < fragments.size(); index++) {
      BaseTimeSeriesPlanNode serverFragment = fragments.get(index);
      serializedPlanFragments.add(Pair.of(serverFragment.getId(), TimeSeriesPlanSerde.serialize(serverFragment)));
    }
    return new TimeSeriesDispatchablePlan(timeSeriesRequest.getLanguage(), serverInstances, fragments.get(0),
        serializedPlanFragments, logicalPlan.getTimeBuckets(), scanVisitorContext.getPlanIdToSegmentsByInstanceId(),
        serverToSegmentsByPlanId);
  }
}
