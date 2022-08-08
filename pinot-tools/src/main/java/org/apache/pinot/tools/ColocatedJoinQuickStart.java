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

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.utils.ZkStarter;
import org.apache.pinot.spi.stream.StreamDataProvider;
import org.apache.pinot.spi.stream.StreamDataServerStartable;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.tools.admin.PinotAdministrator;
import org.apache.pinot.tools.admin.command.QuickstartRunner;
import org.apache.pinot.tools.utils.KafkaStarterUtils;

import static org.apache.pinot.tools.Quickstart.prettyPrintResponse;


public class ColocatedJoinQuickStart extends QuickStartBase {
  private ZkStarter.ZookeeperInstance _zookeeperInstance;
  private StreamDataServerStartable _serverStarter;

  @Override
  public List<String> types() {
    return Collections.singletonList("LOCAL_JOIN");
  }

  @Override
  public Map<String, Object> getConfigOverrides() {
    Map<String, Object> overrides = new HashMap<>(super.getConfigOverrides());
    overrides.put("pinot.multistage.engine.enabled", "true");
    overrides.put("pinot.server.instance.currentDataTableVersion", 4);
    return overrides;
  }

  private QuickstartTableRequest setupTable(File quickstartTmpDir, String tableName)
      throws IOException {
    File riderAttributesBaseDir = new File(quickstartTmpDir, tableName);
    File schemaFile = new File(riderAttributesBaseDir, tableName + "_schema.json");
    File tableConfigFile = new File(riderAttributesBaseDir, tableName + "_offline_table_config.json");
    File ingestionJobSpecFile = new File(riderAttributesBaseDir, "ingestionJobSpec.yaml");
    ClassLoader classLoader = Quickstart.class.getClassLoader();
    URL resource = classLoader.getResource(String.format("examples/batch/%s/%s_schema.json", tableName, tableName));
    Preconditions.checkNotNull(resource);
    FileUtils.copyURLToFile(resource, schemaFile);
    resource = classLoader.getResource(String.format("examples/batch/%s/%s_offline_table_config.json", tableName,
        tableName));
    Preconditions.checkNotNull(resource);
    FileUtils.copyURLToFile(resource, tableConfigFile);
    resource = classLoader.getResource(String.format("examples/batch/%s/ingestionJobSpec.yaml", tableName));
    Preconditions.checkNotNull(resource);
    FileUtils.copyURLToFile(resource, ingestionJobSpecFile);
    QuickstartTableRequest request = new QuickstartTableRequest(riderAttributesBaseDir.getAbsolutePath());
    return request;
  }

  @Override
  public void execute()
      throws Exception {
    File quickstartTmpDir = new File(_dataDir, String.valueOf(System.currentTimeMillis()));

    // Setup two offline tables
    QuickstartTableRequest request1 = setupTable(quickstartTmpDir, "riderAttributes");
    QuickstartTableRequest request2 = setupTable(quickstartTmpDir, "riderAudiences");

    File tempDir = new File(quickstartTmpDir, "tmp");
    FileUtils.forceMkdir(tempDir);
    QuickstartRunner runner =
        new QuickstartRunner(Arrays.asList(request1, request2), 1, 1, 2, 1, tempDir, getConfigOverrides());

    /*
    printStatus(Quickstart.Color.CYAN, "***** Starting Kafka *****");
    startKafka(); */

    printStatus(Quickstart.Color.CYAN, "***** Starting Zookeeper, controller, broker and server *****");
    runner.startAll();
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        printStatus(Quickstart.Color.GREEN, "***** Shutting down offline quick start *****");
        runner.stop();
        FileUtils.deleteDirectory(quickstartTmpDir);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }));
    printStatus(Quickstart.Color.CYAN, "***** Bootstrap riderAttributes table *****");
    runner.bootstrapTable();

    waitForBootstrapToComplete(null);

    Map<String, String> queryOptions = Collections.singletonMap("queryOptions",
        CommonConstants.Broker.Request.QueryOptionKey.USE_MULTISTAGE_ENGINE + "=true");

    printStatus(Quickstart.Color.YELLOW, "***** Multi-stage engine quickstart setup complete *****");
    String q1 = "SELECT count(*) FROM riderAttributes_OFFLINE";
    printStatus(Quickstart.Color.YELLOW, "Total number of documents in the table");
    printStatus(Quickstart.Color.CYAN, "Query : " + q1);
    printStatus(Quickstart.Color.YELLOW, prettyPrintResponse(runner.runQuery(q1, queryOptions)));
    printStatus(Quickstart.Color.GREEN, "***************************************************");

    String q2 = "SELECT /*+ USE_COLOCATED_JOIN */\n"
        + "attr.userUUID, attr.deviceOS, aud.audienceUUID FROM riderAttributes_OFFLINE AS attr JOIN "
        + "riderAudiences_OFFLINE AS aud ON attr.userUUID = aud.userUUID WHERE aud.audienceUUID = 'audience-90' and "
        + "attr.deviceOS = 'android'";
    printStatus(Quickstart.Color.CYAN, "Query : " + q2);
    printStatus(Quickstart.Color.YELLOW, prettyPrintResponse(runner.runQuery(q2, queryOptions)));
    printStatus(Quickstart.Color.GREEN, "***************************************************");

    printStatus(Quickstart.Color.GREEN, "***************************************************");
    printStatus(Quickstart.Color.YELLOW, "Example query run completed.");
    printStatus(Quickstart.Color.GREEN, "***************************************************");
    printStatus(Quickstart.Color.GREEN,
        "You can always go to http://localhost:9000 to play around in the query console");
  }

  private void setupOfflineTable() {
  }

  private void startKafka() {
    _zookeeperInstance = ZkStarter.startLocalZkServer();
    try {
      _serverStarter = StreamDataProvider.getServerDataStartable(KafkaStarterUtils.KAFKA_SERVER_STARTABLE_CLASS_NAME,
          KafkaStarterUtils.getDefaultKafkaConfiguration(_zookeeperInstance));
    } catch (Exception e) {
      throw new RuntimeException("Failed to start " + KafkaStarterUtils.KAFKA_SERVER_STARTABLE_CLASS_NAME, e);
    }
    _serverStarter.start();
    _serverStarter.createTopic("tripEvents", KafkaStarterUtils.getTopicCreationProps(4));
  }

  public static void main(String[] args)
      throws Exception {
    List<String> arguments = new ArrayList<>();
    arguments.addAll(Arrays.asList("QuickStart", "-type", "LOCAL_JOIN"));
    arguments.addAll(Arrays.asList(args));
    PinotAdministrator.main(arguments.toArray(new String[arguments.size()]));
  }
}
