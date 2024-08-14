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

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import org.apache.pinot.tsdb.spi.RangeTimeSeriesRequest;
import org.apache.pinot.tsdb.spi.TimeSeriesLogicalPlanResult;
import org.apache.pinot.tsdb.spi.plan.BaseTimeSeriesPlanNode;
import org.apache.pinot.tsdb.spi.plan.serde.TimeSeriesPlanSerde;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class ExampleTimeSeriesPlannerTest {
  @Test
  public void testPlanGenerationAndSerde()
      throws IOException {
    BaseTimeSeriesPlanNode planNode = null;
    try (InputStream inputStream = ExampleTimeSeriesPlannerTest.class.getClassLoader().getResourceAsStream(
        "plan_test/1.ql")) {
      String resp = new String(inputStream.readAllBytes());
      ExampleTimeSeriesPlanner planner = new ExampleTimeSeriesPlanner();
      RangeTimeSeriesRequest request = new RangeTimeSeriesRequest("example", resp,
          10_000, 11_000, 60, Duration.ofSeconds(10));
      TimeSeriesLogicalPlanResult planResult = planner.plan(request);
      assertNotNull(planResult);
      assertNotNull(planResult.getPlanNode());
      planNode = planResult.getPlanNode();
    }
    BaseTimeSeriesPlanNode deserializedPlanNode = TimeSeriesPlanSerde.deserialize(
        TimeSeriesPlanSerde.serialize(planNode));
    assertNotNull(deserializedPlanNode);
    comparePlanTree(planNode, deserializedPlanNode);
  }

  private void comparePlanTree(BaseTimeSeriesPlanNode tree1, BaseTimeSeriesPlanNode tree2) {
    assertEquals(tree1.getId(), tree2.getId());
    assertEquals(tree1.getKlass(), tree2.getKlass());
    assertEquals(tree1.getExplainName(), tree2.getExplainName());
    assertEquals(tree1.getChildren().size(), tree2.getChildren().size());
    int numChildren = tree1.getChildren().size();
    for (int i = 0; i < numChildren; i++) {
      comparePlanTree(tree1.getChildren().get(i), tree2.getChildren().get(i));
    }
  }
}
