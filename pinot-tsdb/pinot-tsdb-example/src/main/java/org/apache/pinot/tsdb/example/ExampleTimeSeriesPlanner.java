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
package org.apache.pinot.tsdb.example;

import java.util.Map;
import org.apache.pinot.tsdb.spi.RangeTimeSeriesRequest;
import org.apache.pinot.tsdb.spi.TimeSeriesLogicalPlanResult;
import org.apache.pinot.tsdb.spi.TimeSeriesLogicalPlanner;


public class ExampleTimeSeriesPlanner implements TimeSeriesLogicalPlanner {
  @Override
  public void init(Map<String, Object> config) {
  }

  @Override
  public TimeSeriesLogicalPlanResult plan(RangeTimeSeriesRequest request) {
    if (!request.getEngine().equals(Constants.ENGINE_ID)) {
      throw new IllegalArgumentException(String.format("Invalid engine id: %s. Expected: %s", request.getEngine(),
          Constants.ENGINE_ID));
    }
    return null;
  }
}
