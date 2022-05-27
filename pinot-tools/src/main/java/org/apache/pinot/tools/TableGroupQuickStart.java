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

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.utils.ZkStarter;
import org.apache.pinot.spi.stream.StreamDataProvider;
import org.apache.pinot.spi.stream.StreamDataServerStartable;
import org.apache.pinot.tools.admin.PinotAdministrator;
import org.apache.pinot.tools.admin.command.QuickstartRunner;
import org.apache.pinot.tools.streams.githubevents.PullRequestMergedEventsStream;
import org.apache.pinot.tools.utils.KafkaStarterUtils;
import org.apache.pinot.tools.utils.StreamSourceType;


public class TableGroupQuickStart extends QuickStartBase {
  @Override
  public List<String> types() {
    return Arrays.asList("TABLE_GROUP");
  }

  private static final String TAB = "\t\t";
  private static final String NEW_LINE = "\n";
  private static final String DEFAULT_BOOTSTRAP_DIRECTORY = "examples/batch/baseballStats";

  private StreamDataServerStartable _kafkaStarter;

  public enum Color {
    RESET("\u001B[0m"), GREEN("\u001B[32m"), YELLOW("\u001B[33m"), CYAN("\u001B[36m");

    private final String _code;

    public String getCode() {
      return _code;
    }

    Color(String code) {
      _code = code;
    }
  }

  public int getNumMinions() {
    return 0;
  }

  public String getAuthToken() {
    return null;
  }

  public static String prettyPrintResponse(JsonNode response) {
    StringBuilder responseBuilder = new StringBuilder();

    // Sql Results
    if (response.has("resultTable")) {
      JsonNode columns = response.get("resultTable").get("dataSchema").get("columnNames");
      int numColumns = columns.size();
      for (int i = 0; i < numColumns; i++) {
        responseBuilder.append(columns.get(i).asText()).append(TAB);
      }
      responseBuilder.append(NEW_LINE);
      JsonNode rows = response.get("resultTable").get("rows");
      for (int i = 0; i < rows.size(); i++) {
        JsonNode row = rows.get(i);
        for (int j = 0; j < numColumns; j++) {
          responseBuilder.append(row.get(j).asText()).append(TAB);
        }
        responseBuilder.append(NEW_LINE);
      }
    }
    return responseBuilder.toString();
  }

  public void execute()
      throws Exception {
    File quickstartTmpDir = new File(_dataDir, String.valueOf(System.currentTimeMillis()));
    File dataDir = new File("/Users/ankitsultana/.pinot-data/quickstar/");

    QuickstartRunner runner =
        new QuickstartRunner(getTables(), 1, 1, 4,
            getNumMinions(), dataDir, true, getAuthToken(),
            getConfigOverrides(), null, true);
    printStatus(Quickstart.Color.CYAN, "***** Starting Kafka *****");
    final ZkStarter.ZookeeperInstance zookeeperInstance = ZkStarter.startLocalZkServer();
    try {
      _kafkaStarter = StreamDataProvider.getServerDataStartable(KafkaStarterUtils.KAFKA_SERVER_STARTABLE_CLASS_NAME,
          KafkaStarterUtils.getDefaultKafkaConfiguration(zookeeperInstance));
    } catch (Exception e) {
      throw new RuntimeException("Failed to start " + KafkaStarterUtils.KAFKA_SERVER_STARTABLE_CLASS_NAME, e);
    }
    _kafkaStarter.start();
    _kafkaStarter.createTopic("pullRequestMergedEvents", KafkaStarterUtils.getTopicCreationProps(4));
    startGitHubEvents();

    printStatus(Quickstart.Color.CYAN, "***** Starting Zookeeper, controller, broker and server *****");
    runner.startAll();
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        printStatus(Quickstart.Color.GREEN, "***** Shutting down offline quick start *****");
        runner.stop();
        _kafkaStarter.stop();
        FileUtils.deleteDirectory(quickstartTmpDir);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }));
    runner.bootstrapTable();

    waitForBootstrapToComplete(runner);

    printStatus(Quickstart.Color.YELLOW, "***** Offline quickstart setup complete *****");

    if (useDefaultBootstrapTableDir()) {
      // Quickstart is using the default baseballStats sample table, so run sample queries.
      runSampleQueries(runner);
    }

    printStatus(Quickstart.Color.GREEN,
        "You can always go to http://localhost:9000 to play around in the query console");
  }

  private void startStreaming() {
    // Create and Start Kafka
    // Create and Start Data Provider
  }

  private List<QuickstartTableRequest> getTables()
      throws IOException {
    return Arrays.asList(
        getTableRequest("examples/batch/baseballStats"),
        getTableRequest("examples/colocated/baseballStatsColocated"),
        getTableRequest("examples/colocated/dimBaseballTeamsColocated"),
        getRealtimeTableRequest("examples/colocated/githubEventsColocated"));
  }

  private QuickstartTableRequest getTableRequest(String bootstrapDataDir)
      throws IOException {
    String tableName = getTableName(bootstrapDataDir);
    File quickstartTmpDir = new File(_dataDir, String.valueOf(System.currentTimeMillis()));
    File baseDir = new File(quickstartTmpDir, tableName);
    File dataDir = new File(baseDir, "rawdata");
    Preconditions.checkState(dataDir.mkdirs());
    if (useDefaultBootstrapTableDir()) {
      copyResourceTableToTmpDirectory(getBootstrapDataDir(bootstrapDataDir), tableName, baseDir, dataDir, true);
    } else {
      copyFilesystemTableToTmpDirectory(getBootstrapDataDir(bootstrapDataDir), tableName, baseDir);
    }
    return new QuickstartTableRequest(baseDir.getAbsolutePath());
  }

  private void startGitHubEvents()
      throws Exception {
    printStatus(Quickstart.Color.CYAN, "***** Starting  meetup data stream and publishing to Kafka *****");
    printStatus(Quickstart.Color.CYAN,
        String.format("***** Starting pullRequestMergedEvents data stream and publishing to %s *****",
            StreamSourceType.KAFKA));
    File schemaFile = new File("/Users/ankitsultana/code/internal/pinot/pinot-tools/src/main/resources/examples"
        + "/colocated/githubEventsColocated/githubEventsColocated_schema.json");
    String personalAccessToken = System.getenv("GH_TOKEN");
    final PullRequestMergedEventsStream pullRequestMergedEventsStream =
        new PullRequestMergedEventsStream(schemaFile.getAbsolutePath(), "pullRequestMergedEvents", personalAccessToken,
            PullRequestMergedEventsStream.getStreamDataProducer(StreamSourceType.KAFKA));
    pullRequestMergedEventsStream.execute();
    printStatus(Quickstart.Color.CYAN, "***** Waiting for 10 seconds for a few events to get populated *****");
    Thread.sleep(10000);
  }

  private QuickstartTableRequest getRealtimeTableRequest(String bootstrapDataDir)
      throws IOException {
    String tableName = getTableName(bootstrapDataDir);
    File quickstartTmpDir = new File(_dataDir, String.valueOf(System.currentTimeMillis()));
    File baseDir = new File(quickstartTmpDir, tableName);
    copyResourceTableToTmpDirectory(getBootstrapDataDir(bootstrapDataDir), tableName, baseDir, null, false);
    return new QuickstartTableRequest(baseDir.getAbsolutePath());
  }

  private static void copyResourceTableToTmpDirectory(String sourcePath, String tableName, File baseDir, File dataDir,
      boolean isOffline)
      throws IOException {
    String tableConfigSuffix = isOffline ? "_offline_table_config.json" : "_realtime_table_config.json";

    File schemaFile = new File(baseDir, tableName + "_schema.json");
    File tableConfigFile = new File(baseDir, tableName + tableConfigSuffix);
    File ingestionJobSpecFile = new File(baseDir, "ingestionJobSpec.yaml");

    ClassLoader classLoader = Quickstart.class.getClassLoader();
    URL resource = classLoader.getResource(sourcePath + File.separator + tableName + "_schema.json");
    com.google.common.base.Preconditions.checkNotNull(resource);
    FileUtils.copyURLToFile(resource, schemaFile);
    if (isOffline) {
      copyMultipleDataFiles(tableName, sourcePath, dataDir);
    }
    resource = classLoader.getResource(sourcePath + File.separator + "ingestionJobSpec.yaml");
    if (resource != null) {
      FileUtils.copyURLToFile(resource, ingestionJobSpecFile);
    }
    resource = classLoader.getResource(sourcePath + File.separator + tableName + tableConfigSuffix);
    com.google.common.base.Preconditions.checkNotNull(resource);
    FileUtils.copyURLToFile(resource, tableConfigFile);
  }

  private static void copyMultipleDataFiles(String tableName, String sourcePath, File dataDir)
      throws IOException {
    String baseDir = sourcePath + File.separator + "rawdata";
    System.out.println(tableName);
    System.out.println(sourcePath);
    System.out.println(baseDir);
    URL url = Quickstart.class.getClassLoader().getResource(baseDir);
    Preconditions.checkNotNull(url, "couldn't find rawdata resource");
    List<URL> files = getFoo(url, baseDir);
    int cnt = 0;
    for (URL f : files) {
      System.out.println("Copying file: " + f);
      copyDataFile(f, new File(dataDir, tableName + "_" + cnt++ + "_data.csv"));
    }
  }

  private static List<URL> getFoo(URL url, String directoryName)
      throws IOException {
    List<URL> filenames = new ArrayList<>();
    String dirname = directoryName + "/";
    String path = url.getPath();
    String jarPath = path.substring(5, path.indexOf("!"));
    try (JarFile jar = new JarFile(URLDecoder.decode(jarPath, StandardCharsets.UTF_8.name()))) {
      Enumeration<JarEntry> entries = jar.entries();
      while (entries.hasMoreElements()) {
        JarEntry entry = entries.nextElement();
        String name = entry.getName();
        if (name.startsWith(dirname) && !dirname.equals(name)) {
          URL resource = Thread.currentThread().getContextClassLoader().getResource(name);
          filenames.add(resource);
        }
      }
    }
    return filenames;
  }

  private static void copyDataFile(URL resource, File outputFile)
      throws IOException {
    FileUtils.copyURLToFile(resource, outputFile);
  }

  private static void copyFilesystemTableToTmpDirectory(String sourcePath, String tableName, File baseDir)
      throws IOException {
    File fileDb = new File(sourcePath);

    if (!fileDb.exists() || !fileDb.isDirectory()) {
      throw new RuntimeException("Directory " + fileDb.getAbsolutePath() + " not found.");
    }

    File schemaFile = new File(fileDb, tableName + "_schema.json");
    if (!schemaFile.exists()) {
      throw new RuntimeException("Schema file " + schemaFile.getAbsolutePath() + " not found.");
    }

    File tableFile = new File(fileDb, tableName + "_offline_table_config.json");
    if (!tableFile.exists()) {
      throw new RuntimeException("Table table " + tableFile.getAbsolutePath() + " not found.");
    }

    File data = new File(fileDb, "rawdata" + File.separator + tableName + "_data.csv");
    if (!data.exists()) {
      throw new RuntimeException(("Data file " + data.getAbsolutePath() + " not found. "));
    }

    FileUtils.copyDirectory(fileDb, baseDir);
  }

  private static void runSampleQueries(QuickstartRunner runner)
      throws Exception {
    String q1 = "select count(*) from baseballStats limit 1";
    printStatus(Quickstart.Color.YELLOW, "Total number of documents in the table");
    printStatus(Quickstart.Color.CYAN, "Query : " + q1);
    printStatus(Quickstart.Color.YELLOW, prettyPrintResponse(runner.runQuery(q1)));
    printStatus(Quickstart.Color.GREEN, "***************************************************");

    String q2 = "select playerName, sum(runs) from baseballStats group by playerName order by sum(runs) desc limit 5";
    printStatus(Quickstart.Color.YELLOW, "Top 5 run scorers of all time ");
    printStatus(Quickstart.Color.CYAN, "Query : " + q2);
    printStatus(Quickstart.Color.YELLOW, prettyPrintResponse(runner.runQuery(q2)));
    printStatus(Quickstart.Color.GREEN, "***************************************************");

    String q3 =
        "select playerName, sum(runs) from baseballStats where yearID=2000 group by playerName order by sum(runs) "
            + "desc limit 5";
    printStatus(Quickstart.Color.YELLOW, "Top 5 run scorers of the year 2000");
    printStatus(Quickstart.Color.CYAN, "Query : " + q3);
    printStatus(Quickstart.Color.YELLOW, prettyPrintResponse(runner.runQuery(q3)));
    printStatus(Quickstart.Color.GREEN, "***************************************************");

    String q4 =
        "select playerName, sum(runs) from baseballStats where yearID>=2000 group by playerName order by sum(runs) "
            + "desc limit 10";
    printStatus(Quickstart.Color.YELLOW, "Top 10 run scorers after 2000");
    printStatus(Quickstart.Color.CYAN, "Query : " + q4);
    printStatus(Quickstart.Color.YELLOW, prettyPrintResponse(runner.runQuery(q4)));
    printStatus(Quickstart.Color.GREEN, "***************************************************");

    String q5 = "select playerName, runs, homeRuns from baseballStats order by yearID limit 10";
    printStatus(Quickstart.Color.YELLOW,
        "Print playerName,runs,homeRuns for 10 records from the table and order them by yearID");
    printStatus(Quickstart.Color.CYAN, "Query : " + q5);
    printStatus(Quickstart.Color.YELLOW, prettyPrintResponse(runner.runQuery(q5)));
    printStatus(Quickstart.Color.GREEN, "***************************************************");
  }

  public static void main(String[] args)
      throws Exception {
    List<String> arguments = new ArrayList<>();
    arguments.addAll(Arrays.asList("QuickStart", "-type", "TABLEGROUP"));
    arguments.addAll(Arrays.asList(args));
    PinotAdministrator.main(arguments.toArray(new String[arguments.size()]));
  }
}
