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
package org.apache.pinot.tsdb.planner.physical;

import java.util.List;
import java.util.Map;
import org.apache.pinot.tsdb.spi.TimeBuckets;
import org.apache.pinot.tsdb.spi.plan.BaseTimeSeriesPlanNode;


public class TimeSeriesDispatchablePlan {
  private final List<TimeSeriesQueryServerInstance> _queryServerInstance;
  private final String _language;
  private final BaseTimeSeriesPlanNode _brokerFragment;
  private final List<String> _serializedPlansInOrder;
  private final TimeBuckets _timeBuckets;
  private final Map<String, Map<String, List<String>>> _planIdToSegments;

  public TimeSeriesDispatchablePlan(String language, List<TimeSeriesQueryServerInstance> serverInstances,
      BaseTimeSeriesPlanNode brokerFragment, List<String> serializedPlanMap, TimeBuckets timeBuckets,
      Map<String, Map<String, List<String>>> planIdToSegments) {
    _language = language;
    _queryServerInstance = serverInstances;
    _brokerFragment = brokerFragment;
    _serializedPlansInOrder = serializedPlanMap;
    _timeBuckets = timeBuckets;
    _planIdToSegments = planIdToSegments;
  }

  public String getLanguage() {
    return _language;
  }

  public List<TimeSeriesQueryServerInstance> getQueryServers() {
    return _queryServerInstance;
  }

  public BaseTimeSeriesPlanNode getBrokerFragment() {
    return _brokerFragment;
  }

  public List<String> getSerializedPlanFragmentByRootId() {
    return _serializedPlansInOrder;
  }

  public TimeBuckets getTimeBuckets() {
    return _timeBuckets;
  }

  public Map<String, Map<String, List<String>>> getPlanIdToSegments() {
    return _planIdToSegments;
  }
}
