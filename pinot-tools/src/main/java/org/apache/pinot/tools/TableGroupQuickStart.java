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
import java.net.URLDecoder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.pinot.common.utils.ZkStarter;
import org.apache.pinot.spi.stream.StreamDataServerStartable;
import org.apache.pinot.spi.utils.builder.ControllerRequestURLBuilder;
import org.apache.pinot.tools.admin.PinotAdministrator;
import org.apache.pinot.tools.admin.command.AbstractBaseAdminCommand;
import org.apache.pinot.tools.admin.command.QuickstartRunner;
import org.apache.pinot.tools.streams.githubevents.PullRequestMergedEventsStream;
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

  public void execute()
      throws Exception {
    File quickstartTmpDir = new File(_dataDir, String.valueOf(System.currentTimeMillis()));
    File dataDir = new File("/Users/ankitsultana/.pinot-data/quickstar/");

    QuickstartRunner runner =
        new QuickstartRunner(getTables(), 1, 1, 4,
            getNumMinions(), dataDir, true, null,
            getConfigOverrides(), null, true);
    printStatus(Quickstart.Color.CYAN, "***** Starting Kafka *****");
    final ZkStarter.ZookeeperInstance zookeeperInstance = ZkStarter.startLocalZkServer();
    /*
    try {
      _kafkaStarter = StreamDataProvider.getServerDataStartable(KafkaStarterUtils.KAFKA_SERVER_STARTABLE_CLASS_NAME,
          KafkaStarterUtils.getDefaultKafkaConfiguration(zookeeperInstance));
    } catch (Exception e) {
      throw new RuntimeException("Failed to start " + KafkaStarterUtils.KAFKA_SERVER_STARTABLE_CLASS_NAME, e);
    }
    _kafkaStarter.start();
    _kafkaStarter.createTopic("pullRequestMergedEvents", KafkaStarterUtils.getTopicCreationProps(4));
    startGitHubEvents(); */

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
    if (!createTableGroup()) {
      printStatus(Quickstart.Color.YELLOW, "***** Error creating table-group *****");
      Thread.sleep(1000 * 600);
      throw new RuntimeException("Error creating table-group");
    }
    runner.bootstrapTable();

    waitForBootstrapToComplete(runner);

    printStatus(Quickstart.Color.YELLOW, "***** Offline quickstart setup complete *****");

    printStatus(Quickstart.Color.GREEN,
        "You can always go to http://localhost:9000 to play around in the query console");
  }

  private List<QuickstartTableRequest> getTables()
      throws IOException {
    return Arrays.asList(
        getTableRequest("examples/batch/baseballStats"),
        getTableRequest("examples/batch/riderAttributes"),
        getTableRequest("examples/batch/riderAudiences"));
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

  private boolean createTableGroup()
      throws IOException {
    String controllerAddress = "http://localhost:9000";
    String tableGroupConfigStr = IOUtils.toString(this.getClass().getClassLoader().getResourceAsStream("conf"
            + "/sample_group_config.json"),
        Charset.defaultCharset());
    String res = AbstractBaseAdminCommand
        .sendRequest("POST", ControllerRequestURLBuilder.baseUrl(controllerAddress).forGroupCreate(),
            tableGroupConfigStr,
            new ArrayList<>());
    return res.contains("successfully");
  }

  public static void main(String[] args)
      throws Exception {
    List<String> arguments = new ArrayList<>();
    arguments.addAll(Arrays.asList("QuickStart", "-type", "TABLEGROUP"));
    arguments.addAll(Arrays.asList(args));
    PinotAdministrator.main(arguments.toArray(new String[arguments.size()]));
  }
}
