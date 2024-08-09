package org.apache.pinot.core.query.executor;

import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.net.URL;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.helix.HelixManager;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.TimeSeriesContext;
import org.apache.pinot.core.data.manager.InstanceDataManager;
import org.apache.pinot.core.data.manager.offline.TableDataManagerProvider;
import org.apache.pinot.core.operator.blocks.InstanceResponseBlock;
import org.apache.pinot.core.operator.blocks.results.TimeSeriesResultsBlock;
import org.apache.pinot.core.query.request.ServerQueryRequest;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.SegmentTestUtils;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.utils.SegmentLocks;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.creator.SegmentIndexCreationDriver;
import org.apache.pinot.spi.config.instance.InstanceDataManagerConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.StreamIngestionConfig;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.MetricFieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.FileFormat;
import org.apache.pinot.spi.env.CommonsConfigurationUtils;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.tsdb.example.series.ExampleSeriesBuilderFactory;
import org.apache.pinot.tsdb.spi.AggInfo;
import org.apache.pinot.tsdb.spi.PinotTimeSeriesConfigs;
import org.apache.pinot.tsdb.spi.TimeBuckets;
import org.apache.pinot.tsdb.spi.series.Series;
import org.apache.pinot.tsdb.spi.series.SeriesBlock;
import org.apache.pinot.tsdb.spi.series.SeriesBuilderFactoryProvider;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


public class QueryExecutorTimeSeriesTest {
  private static final String QUERY_EXECUTOR_CONFIG_PATH = "conf/query-executor.properties";
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "QueryExecutorTimeSeriesTest");
  private static final String CSV_DATA_PATH = "data/time_series_test_data.csv";
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String TIME_COLUMN_NAME = "order_timestamp";
  private static final String METRIC_COLUMN_NAME = "order_dollar_amount_e2";
  private static final String DIM_COLUMN_NAME = "merchant_id";
  private static final String REALTIME_TABLE_NAME = TableNameBuilder.REALTIME.tableNameWithType(RAW_TABLE_NAME);
  private static final int NUM_SEGMENTS_TO_GENERATE = 2;
  private static final ExecutorService QUERY_RUNNERS = Executors.newFixedThreadPool(20);

  private final List<ImmutableSegment> _indexSegments = new ArrayList<>(NUM_SEGMENTS_TO_GENERATE);
  private final List<String> _segmentNames = new ArrayList<>(NUM_SEGMENTS_TO_GENERATE);

  private QueryExecutor _queryExecutor;

  @BeforeClass
  public void setUp()
      throws Exception {
    ServerMetrics.register(mock(ServerMetrics.class));
    // Set up the segments
    FileUtils.deleteQuietly(TEMP_DIR);
    assertTrue(TEMP_DIR.mkdirs());
    URL resourceUrl = getClass().getClassLoader().getResource(CSV_DATA_PATH);
    Assert.assertNotNull(resourceUrl);

    File csvFile = new File(resourceUrl.getFile());
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setStreamIngestionConfig(
        new StreamIngestionConfig(Collections.singletonList(ImmutableMap.of())));
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(RAW_TABLE_NAME)
        .setIngestionConfig(ingestionConfig).build();
    Schema schema = new Schema();
    schema.addField(new DimensionFieldSpec(TIME_COLUMN_NAME, FieldSpec.DataType.LONG, true));
    schema.addField(new MetricFieldSpec(METRIC_COLUMN_NAME, FieldSpec.DataType.LONG));
    schema.addField(new DimensionFieldSpec(DIM_COLUMN_NAME, FieldSpec.DataType.STRING, true));
    File tableDataDir = new File(TEMP_DIR, REALTIME_TABLE_NAME);
    int i = 0;
    for (; i < NUM_SEGMENTS_TO_GENERATE; i++) {
      SegmentGeneratorConfig config =
          SegmentTestUtils.getSegmentGeneratorConfig(csvFile, FileFormat.CSV, tableDataDir, RAW_TABLE_NAME,
              tableConfig, schema);
      config.setSegmentNamePostfix(Integer.toString(i));
      SegmentIndexCreationDriver driver = new SegmentIndexCreationDriverImpl();
      driver.init(config);
      driver.build();
      _indexSegments.add(ImmutableSegmentLoader.load(new File(tableDataDir, driver.getSegmentName()), ReadMode.mmap));
      _segmentNames.add(driver.getSegmentName());
    }

    // Mock the instance data manager
    InstanceDataManagerConfig instanceDataManagerConfig = mock(InstanceDataManagerConfig.class);
    when(instanceDataManagerConfig.getInstanceDataDir()).thenReturn(TEMP_DIR.getAbsolutePath());
    TableDataManager tableDataManager =
        new TableDataManagerProvider(instanceDataManagerConfig, mock(HelixManager.class),
            new SegmentLocks()).getTableDataManager(tableConfig);
    tableDataManager.start();
    for (ImmutableSegment indexSegment : _indexSegments) {
      tableDataManager.addSegment(indexSegment);
    }
    InstanceDataManager instanceDataManager = mock(InstanceDataManager.class);
    when(instanceDataManager.getTableDataManager(REALTIME_TABLE_NAME)).thenReturn(tableDataManager);

    // Set up the query executor
    resourceUrl = getClass().getClassLoader().getResource(QUERY_EXECUTOR_CONFIG_PATH);
    Assert.assertNotNull(resourceUrl);
    PropertiesConfiguration queryExecutorConfig = CommonsConfigurationUtils.fromFile(new File(resourceUrl.getFile()));
    _queryExecutor = new ServerQueryExecutorV1Impl();
    _queryExecutor.init(new PinotConfiguration(queryExecutorConfig), instanceDataManager, ServerMetrics.get());

    setUpTimeSeriesEngine();
  }

  public void setUpTimeSeriesEngine() {
    SeriesBuilderFactoryProvider sbfProvider = SeriesBuilderFactoryProvider.INSTANCE;
    PinotConfiguration pinotConfiguration = new PinotConfiguration(ImmutableMap.of(
        PinotTimeSeriesConfigs.CommonConfigs.TIME_SERIES_ENGINES, "example",
        PinotTimeSeriesConfigs.TIME_SERIES_ENGINE_CONFIG_PREFIX + ".example.series.builder.class",
       ExampleSeriesBuilderFactory.class.getName()
    ));
    sbfProvider.init(pinotConfiguration);
  }

  @Test
  public void testTimeSeriesQuery() {
    TimeBuckets timeBuckets = TimeBuckets.ofSeconds(100L, Duration.ofSeconds(2), 10);
    TimeSeriesContext timeSeriesContext = new TimeSeriesContext(
        "example", TIME_COLUMN_NAME, TimeUnit.MILLISECONDS, timeBuckets, 0L,
        ExpressionContext.forIdentifier(METRIC_COLUMN_NAME),
        new AggInfo("SUM", true));
    QueryContext queryContext = getQueryContextForTimeSeries(timeSeriesContext);
    ServerQueryRequest serverQueryRequest = new ServerQueryRequest(
        queryContext, _segmentNames, new HashMap<>(), ServerMetrics.get());
    InstanceResponseBlock instanceResponse = _queryExecutor.execute(serverQueryRequest, QUERY_RUNNERS);
    assertTrue(instanceResponse.getResultsBlock() instanceof TimeSeriesResultsBlock);
    SeriesBlock seriesBlock = ((TimeSeriesResultsBlock) instanceResponse.getResultsBlock()).getSeriesBlock();
    assertEquals(seriesBlock.getSeriesMap().size(), 1L);
    assertEquals(seriesBlock.getSeriesMap().values().iterator().next().size(), 1L);
    Series series = seriesBlock.getSeriesMap().values().iterator().next().get(0);
    assertEquals(series.getValues()[0], 3000L);
    assertEquals(series.getValues()[1], 3000L);
    assertEquals(series.getValues()[2], 1000L);
    for (int index = 3; index < 10; index++) {
      assertNull(series.getValues()[index]);
    }
  }

  private QueryContext getQueryContextForTimeSeries(TimeSeriesContext context) {
    QueryContext.Builder builder = new QueryContext.Builder();
    builder.setTableName(REALTIME_TABLE_NAME);
    builder.setTimeSeriesContext(context);
    builder.setAliasList(Collections.emptyList());
    builder.setSelectExpressions(Collections.emptyList());
    return builder.build();
  }
}
