package org.apache.pinot.tsdb.example;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import org.apache.pinot.tsdb.spi.RangeTimeSeriesRequest;
import org.apache.pinot.tsdb.spi.TimeSeriesLogicalPlanResult;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class ExampleTimeSeriesPlannerTest {
  @Test
  public void testFoo()
      throws IOException {
    try (InputStream inputStream = ExampleTimeSeriesPlannerTest.class.getClassLoader().getResourceAsStream(
        "plan_test/1.ql")) {
      String resp = new String(inputStream.readAllBytes());
      ExampleTimeSeriesPlanner planner = new ExampleTimeSeriesPlanner();
      RangeTimeSeriesRequest request = new RangeTimeSeriesRequest("example", resp,
          10_000, 11_000, 60, Duration.ofSeconds(10));
      TimeSeriesLogicalPlanResult planResult = planner.plan(request);
      assertNotNull(planResult);
    }
  }
}
