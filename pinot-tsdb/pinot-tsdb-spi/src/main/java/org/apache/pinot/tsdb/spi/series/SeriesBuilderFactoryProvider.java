package org.apache.pinot.tsdb.spi.series;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.tsdb.spi.PinotTimeSeriesConfigs;


public class SeriesBuilderFactoryProvider {
  public static final SeriesBuilderFactoryProvider INSTANCE = new SeriesBuilderFactoryProvider();
  private static final Map<String, SeriesBuilderFactory> FACTORY_MAP = new HashMap<>();

  private SeriesBuilderFactoryProvider() {
  }

  public void init(PinotConfiguration pinotConfiguration) {
    String[] engines = pinotConfiguration.getProperty(
        PinotTimeSeriesConfigs.CommonConfigs.TIME_SERIES_ENGINES).split(",");
    for (String engine : engines) {
      String seriesBuilderClass = pinotConfiguration
          .getProperty(PinotTimeSeriesConfigs.CommonConfigs.getSeriesBuilderClass(engine));
      try {
        Object untypedSeriesBuilderFactory = Class.forName(seriesBuilderClass).getConstructor().newInstance();
        if (!(untypedSeriesBuilderFactory instanceof SeriesBuilderFactory)) {
          throw new RuntimeException("Series builder factory class " + seriesBuilderClass
              + " does not implement SeriesBuilderFactory");
        }
        SeriesBuilderFactory seriesBuilderFactory = (SeriesBuilderFactory) untypedSeriesBuilderFactory;
        seriesBuilderFactory.init(pinotConfiguration.subset(
            PinotTimeSeriesConfigs.TIME_SERIES_ENGINE_CONFIG_PREFIX + "." + engine));
        FACTORY_MAP.put(engine, seriesBuilderFactory);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  public static SeriesBuilderFactory getSeriesBuilderFactory(String engine) {
    return Objects.requireNonNull(FACTORY_MAP.get(engine),
        "No series builder factory found for engine: " + engine);
  }
}
