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
package org.apache.pinot.tools;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.tsdb.spi.PinotTimeSeriesConfigs;


public class TimeSeriesEngineQuickStart extends Quickstart {
  private static final String[] TIME_SERIES_TABLE_DIRECTORIES = new String[]{
      "examples/batch/airlineStats",
      "examples/batch/baseballStats",
      "examples/batch/billing",
      "examples/batch/dimBaseballTeams",
      "examples/batch/githubEvents",
      "examples/batch/githubComplexTypeEvents",
      "examples/batch/ssb/customer",
      "examples/batch/ssb/dates",
      "examples/batch/ssb/lineorder",
      "examples/batch/ssb/part",
      "examples/batch/ssb/supplier",
      "examples/batch/starbucksStores",
      "examples/batch/fineFoodReviews",
  };

  @Override
  public String[] getDefaultBatchTableDirectories() {
    return TIME_SERIES_TABLE_DIRECTORIES;
  }

  @Override
  public List<String> types() {
    return Collections.singletonList("TIME_SERIES");
  }

  @Override
  protected Map<String, Object> getConfigOverrides() {
    Map<String, Object> configs = new HashMap<>();
    configs.put(PinotTimeSeriesConfigs.CommonConfigs.getEnableEngines(), "example");
    configs.put(PinotTimeSeriesConfigs.CommonConfigs.getSeriesBuilderClass("example"),
        "org.apache.pinot.tsdb.example.series.ExampleSeriesBuilderFactory");
    return configs;
  }

  @Override
  protected int getNumQuickstartRunnerServers() {
    return 2;
  }
}
