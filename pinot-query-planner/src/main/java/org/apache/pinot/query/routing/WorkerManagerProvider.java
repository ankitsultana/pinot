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

import java.util.List;
import java.util.Map;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.core.routing.RoutingManager;
import org.apache.pinot.query.planner.QueryPlan;


public class WorkerManagerProvider {
  private String _hostname;
  private int _port;
  private RoutingManager _routingManager;
  private TableCache _tableCache;

  public WorkerManagerProvider(String hostname, int port, RoutingManager routingManager, TableCache tableCache) {
    _hostname = hostname;
    _port = port;
    _routingManager = routingManager;
    _tableCache = tableCache;
  }

  public PartitionWorkerManager get(QueryPlan queryPlan, Map<Integer, List<Integer>> stageTree, long requestId,
      Map<String, String> options) {
    return new PartitionWorkerManager(_hostname, _port, _routingManager, queryPlan, stageTree, requestId, options,
        _tableCache);
  }
}
