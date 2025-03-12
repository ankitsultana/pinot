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
package org.apache.pinot.query.context;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.core.routing.RoutingManager;
import org.apache.pinot.core.routing.TimeBoundaryInfo;
import org.apache.pinot.query.planner.logical.rel2plan.PlanIdGenerator;
import org.apache.pinot.query.routing.QueryServerInstance;
import org.apache.pinot.query.routing.WorkerManager;


public class PhysicalPlannerContext {
  private final Map<String, QueryServerInstance> _instanceIdToQueryServerInstance = new HashMap<>();
  private final Map<Integer, Map<String, Set<String>>> _unavailableSegmentsMap = new HashMap<>();
  private final Map<Integer, Map<Integer, Map<String, List<String>>>> _workerIdToSegmentsMap = new HashMap<>();
  private final Map<Integer, TimeBoundaryInfo> _timeBoundaryInfoMap = new HashMap<>();
  private final Map<Integer, Map<String, String>> _tableOptionsMap = new HashMap<>();
  private final Map<Integer, Set<String>> _scannedTableMap = new HashMap<>();
  private final PlanIdGenerator _idGenerator;
  private final RoutingManager _routingManager;
  private final String _hostName;
  private final int _port;
  private final long _requestId;

  public PhysicalPlannerContext(@Nullable WorkerManager workerManager) {
    if (workerManager == null) {
      _routingManager = null;
      _hostName = null;
      _port = 0;
      _requestId = 0;
    } else {
      _routingManager = workerManager.getRoutingManager();
      _hostName = workerManager.getHostName();
      _port = workerManager.getPort();
      _requestId = 0;
    }
  }

  public int getNextId() {
    return _idGenerator.get();
  }

  public long getRequestId() {
    return _requestId;
  }

  public int getPort() {
    return _port;
  }

  public String getHostName() {
    return _hostName;
  }

  public Map<String, QueryServerInstance> getInstanceIdToQueryServerInstance() {
    return _instanceIdToQueryServerInstance;
  }

  public RoutingManager getRoutingManager() {
    return _routingManager;
  }

  public Map<Integer, Map<String, Set<String>>> getUnavailableSegmentsMap() {
    return _unavailableSegmentsMap;
  }

  public Map<Integer, Map<Integer, Map<String, List<String>>>> getWorkerIdToSegmentsMap() {
    return _workerIdToSegmentsMap;
  }

  public Map<Integer, TimeBoundaryInfo> getTimeBoundaryInfoMap() {
    return _timeBoundaryInfoMap;
  }

  public Map<Integer, Map<String, String>> getTableOptionsMap() {
    return _tableOptionsMap;
  }

  public Map<Integer, Set<String>> getScannedTableMap() {
    return _scannedTableMap;
  }
}
