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
package org.apache.pinot.common.metrics;

import com.yammer.metrics.core.MetricName;
import java.util.concurrent.TimeUnit;
import java.util.function.IntConsumer;
import java.util.stream.IntStream;
import org.apache.pinot.plugin.metrics.yammer.YammerMetricsRegistry;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.metrics.PinotMeter;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.apache.pinot.spi.utils.CommonConstants.CONFIG_OF_METRICS_FACTORY_CLASS_NAME;


public class AbstractMetricsTest {
  @Test
  public void testAddOrUpdateGauge() {
    PinotConfiguration pinotConfiguration = new PinotConfiguration();
    pinotConfiguration.setProperty(CONFIG_OF_METRICS_FACTORY_CLASS_NAME,
        "org.apache.pinot.plugin.metrics.yammer.YammerMetricsFactory");
    PinotMetricUtils.init(pinotConfiguration);
    ControllerMetrics controllerMetrics = new ControllerMetrics(new YammerMetricsRegistry());
    String metricName = "test";
    // add gauge
    controllerMetrics.setOrUpdateGauge(metricName, () -> 1L);
    Assert.assertEquals(MetricValueUtils.getGaugeValue(controllerMetrics, metricName), 1);

    // update gauge
    controllerMetrics.setOrUpdateGauge(metricName, () -> 2L);
    Assert.assertEquals(MetricValueUtils.getGaugeValue(controllerMetrics, metricName), 2);

    // remove gauge
    controllerMetrics.removeGauge(metricName);
    Assert.assertTrue(controllerMetrics.getMetricsRegistry().allMetrics().isEmpty());
  }

  @Test
  public void testMultipleUpdatesToGauge() throws InterruptedException {
    PinotConfiguration pinotConfiguration = new PinotConfiguration();
    pinotConfiguration.setProperty(CONFIG_OF_METRICS_FACTORY_CLASS_NAME,
        "org.apache.pinot.plugin.metrics.yammer.YammerMetricsFactory");
    PinotMetricUtils.init(pinotConfiguration);
    ControllerMetrics controllerMetrics = new ControllerMetrics(new YammerMetricsRegistry());
    String metricName = "testMultipleUpdates";

    // update and remove gauge simultaneously
    IntStream.range(0, 1000).forEach(i -> {
      controllerMetrics.setOrUpdateGauge(metricName, () -> (long) i);
    });

    // Verify final value
    Assert.assertEquals(MetricValueUtils.getGaugeValue(controllerMetrics, metricName), 999);
    // remove gauge
    controllerMetrics.removeGauge(metricName);
    Assert.assertTrue(controllerMetrics.getMetricsRegistry().allMetrics().isEmpty());
  }

  @Test
  public void testRemoveNonExistentGauge() {
    PinotConfiguration pinotConfiguration = new PinotConfiguration();
    pinotConfiguration.setProperty(CONFIG_OF_METRICS_FACTORY_CLASS_NAME,
        "org.apache.pinot.plugin.metrics.yammer.YammerMetricsFactory");
    PinotMetricUtils.init(pinotConfiguration);
    ControllerMetrics controllerMetrics = new ControllerMetrics(new YammerMetricsRegistry());
    String metricName = "testNonExistent";

    // Attempt to remove a nonexistent gauge
    controllerMetrics.removeGauge(metricName);
    Assert.assertTrue(controllerMetrics.getMetricsRegistry().allMetrics().isEmpty());
  }

  @Test
  public void testMultipleGauges() {
    PinotConfiguration pinotConfiguration = new PinotConfiguration();
    pinotConfiguration.setProperty(CONFIG_OF_METRICS_FACTORY_CLASS_NAME,
        "org.apache.pinot.plugin.metrics.yammer.YammerMetricsFactory");
    PinotMetricUtils.init(pinotConfiguration);
    ControllerMetrics controllerMetrics = new ControllerMetrics(new YammerMetricsRegistry());
    String metricName1 = "testMultiple1";
    String metricName2 = "testMultiple2";

    // Add multiple gauges
    controllerMetrics.setOrUpdateGauge(metricName1, () -> 1L);
    controllerMetrics.setOrUpdateGauge(metricName2, () -> 2L);

    // Verify values
    Assert.assertEquals(MetricValueUtils.getGaugeValue(controllerMetrics, metricName1), 1);
    Assert.assertEquals(MetricValueUtils.getGaugeValue(controllerMetrics, metricName2), 2);

    // Remove gauges
    controllerMetrics.removeGauge(metricName1);
    controllerMetrics.removeGauge(metricName2);
    Assert.assertTrue(controllerMetrics.getMetricsRegistry().allMetrics().isEmpty());
  }

  /**
   * Creates and initializes a concrete instance of {@link AbstractMetrics} (in this case, a {@code ControllerMetrics}).
   * @return a {@code ControllerMetrics} suitable for testing {@code AbstractMetrics} APIs
   */
  private static ControllerMetrics buildTestMetrics() {
    PinotConfiguration pinotConfiguration = new PinotConfiguration();
    pinotConfiguration.setProperty(CONFIG_OF_METRICS_FACTORY_CLASS_NAME,
        "org.apache.pinot.plugin.metrics.yammer.YammerMetricsFactory");
    PinotMetricUtils.init(pinotConfiguration);
    return new ControllerMetrics(new YammerMetricsRegistry());
  }

  /**
   * Tests the {@link AbstractMetrics} APIs relating to query phases
   */
  @Test
  public void testQueryPhases() {
    final ControllerMetrics testMetrics = buildTestMetrics();
    final MetricsInspector inspector = new MetricsInspector(testMetrics.getMetricsRegistry());

    // Establish dummy values to be used in the test
    final AbstractMetrics.QueryPhase testPhase = () -> "testPhase";
    Assert.assertEquals(testPhase.getDescription(), "");
    Assert.assertEquals(testPhase.getQueryPhaseName(), "testPhase");
    final String testTableName = "tbl_testQueryPhases";
    final String testTableName2 = "tbl2_testQueryPhases";

    // Add a phase timing, check for correctness
    testMetrics.addPhaseTiming(testTableName, testPhase, 1, TimeUnit.SECONDS);
    final MetricName tbl1Metric = inspector.lastMetric();
    Assert.assertEquals(inspector.getTimer(tbl1Metric).sum(), 1000);

    // Add to the existing timer, using different API
    testMetrics.addPhaseTiming(testTableName, testPhase, 444000000 /* nanoseconds */);
    Assert.assertEquals(inspector.getTimer(tbl1Metric).sum(), 1444);

    // Add phase timing to a different table. Verify new timer is set up correctly, old timer is not affected
    testMetrics.addPhaseTiming(testTableName2, testPhase, 22, TimeUnit.MILLISECONDS);
    final MetricName tbl2Metric = inspector.lastMetric();
    Assert.assertEquals(inspector.getTimer(tbl2Metric).sum(), 22);
    Assert.assertEquals(inspector.getTimer(tbl1Metric).sum(), 1444);

    // Remove both timers. Verify the metrics registry is now empty
    testMetrics.removePhaseTiming(testTableName, testPhase);
    testMetrics.removePhaseTiming(testTableName2, testPhase);
    Assert.assertTrue(testMetrics.getMetricsRegistry().allMetrics().isEmpty());
  }

  /**
   * Tests the {@link AbstractMetrics} APIs relating to timer metrics
   */
  @Test
  public void testTimerMetrics() {
    ControllerMetrics testMetrics = buildTestMetrics();
    MetricsInspector inspector = new MetricsInspector(testMetrics.getMetricsRegistry());
    String tableName = "tbl_testTimerMetrics";
    String keyName = "keyName";
    ControllerTimer timer = ControllerTimer.IDEAL_STATE_UPDATE_TIME_MS;

    // Test timed table APIs
    testMetrics.addTimedTableValue(tableName, timer, 6, TimeUnit.SECONDS);
    final MetricName t1Metric = inspector.lastMetric();
    Assert.assertEquals(inspector.getTimer(t1Metric).sum(), 6000);
    testMetrics.addTimedTableValue(tableName, keyName, timer, 500, TimeUnit.MILLISECONDS);
    final MetricName t2Metric = inspector.lastMetric();
    Assert.assertEquals(inspector.getTimer(t2Metric).sum(), 500);

    // Test timed value APIs
    testMetrics.addTimedValue(timer, 40, TimeUnit.MILLISECONDS);
    final MetricName t3Metric = inspector.lastMetric();
    Assert.assertEquals(inspector.getTimer(t3Metric).sum(), 40);
    testMetrics.addTimedValue(keyName, timer, 3, TimeUnit.MILLISECONDS);
    final MetricName t4Metric = inspector.lastMetric();
    Assert.assertEquals(inspector.getTimer(t4Metric).sum(), 3);

    // Remove added timers and verify the metrics registry is now empty
    Assert.assertEquals(testMetrics.getMetricsRegistry().allMetrics().size(), 4);
    testMetrics.removeTableTimer(tableName, timer);
    testMetrics.removeTimer(t2Metric.getName());
    testMetrics.removeTimer(t3Metric.getName());
    testMetrics.removeTimer(t4Metric.getName());
    Assert.assertTrue(testMetrics.getMetricsRegistry().allMetrics().isEmpty());
  }

  /**
   * Tests the {@link AbstractMetrics} APIs relating to metered metrics
   */
  @Test
  public void testMeteredMetrics() {
    final ControllerMetrics testMetrics = buildTestMetrics();
    final MetricsInspector inspector = new MetricsInspector(testMetrics.getMetricsRegistry());
    final String tableName = "tbl_testMeteredMetrics";
    final String keyName = "keyName";
    final ControllerMeter meter = ControllerMeter.CONTROLLER_INSTANCE_POST_ERROR;
    final ControllerMeter meter2 = ControllerMeter.CONTROLLER_PERIODIC_TASK_ERROR;

    // Holder for the most recently seen Metric
    final MetricName[] currentMetric = new MetricName[1];

    // When a new metric is expected we'll call this lambda to assert the metric was created, and update currentMetric
    final Runnable expectNewMetric = () -> {
      Assert.assertNotEquals(inspector.lastMetric(), currentMetric[0]);
      currentMetric[0] = inspector.lastMetric();
    };

    // Lambda to verify that the latest metric has the expected value, and its creation was expected
    final IntConsumer expectMeteredCount = expected -> {
      Assert.assertEquals(inspector.getMetered(currentMetric[0]).count(), expected);
      Assert.assertEquals(currentMetric[0], inspector.lastMetric());
    };

    // Test global meter APIs
    testMetrics.addMeteredGlobalValue(meter, 5);
    expectNewMetric.run();
    expectMeteredCount.accept(5);
    testMetrics.addMeteredGlobalValue(meter, 4, testMetrics.getMeteredValue(meter));
    expectMeteredCount.accept(9);

    // Test meter with key APIs
    testMetrics.addMeteredValue(keyName, meter, 9);
    expectNewMetric.run();
    expectMeteredCount.accept(9);
    PinotMeter reusedMeter = testMetrics.addMeteredValue(keyName, meter2, 13, null);
    expectNewMetric.run();
    expectMeteredCount.accept(13);
    testMetrics.addMeteredValue(keyName, meter2, 6, reusedMeter);
    expectMeteredCount.accept(19);

    // Test table-level meter APIs
    testMetrics.addMeteredTableValue(tableName, meter, 15);
    expectNewMetric.run();
    expectMeteredCount.accept(15);
    testMetrics.addMeteredTableValue(tableName, meter2, 3, testMetrics.getMeteredTableValue(tableName, meter));
    expectMeteredCount.accept(18);

    // Test table-level meter with additional key APIs
    testMetrics.addMeteredTableValue(tableName, keyName, meter, 21);
    expectNewMetric.run();
    expectMeteredCount.accept(21);
    reusedMeter = testMetrics.addMeteredTableValue(tableName, keyName, meter2, 23, null);
    expectNewMetric.run();
    expectMeteredCount.accept(23);
    testMetrics.addMeteredTableValue(tableName, keyName, meter2, 5, reusedMeter);
    expectMeteredCount.accept(28);

    // Test removal APIs
    Assert.assertEquals(testMetrics.getMetricsRegistry().allMetrics().size(), 6);
    // This is the only AbstractMetrics method for removing Meter-type metrics. Should others be added?
    testMetrics.removeTableMeter(tableName, meter);
    Assert.assertEquals(testMetrics.getMetricsRegistry().allMetrics().size(), 5);
    // If we do add other cleanup APIs to AbstractMetrics, they should be tested here. For now, clean the remaining
    // metrics with generic APIs.
    testMetrics.getMetricsRegistry().allMetrics().keySet().forEach(testMetrics.getMetricsRegistry()::removeMetric);
    Assert.assertTrue(testMetrics.getMetricsRegistry().allMetrics().isEmpty());
  }
}
