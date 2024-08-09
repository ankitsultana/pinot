package org.apache.pinot.tsdb.spi;

public class PinotTimeSeriesConfigs {
  private PinotTimeSeriesConfigs() {
  }

  public static final String TIME_SERIES_ENGINE_CONFIG_PREFIX = "pinot.time.series";

  public static class CommonConfigs {
    public static final String TIME_SERIES_ENGINES = TIME_SERIES_ENGINE_CONFIG_PREFIX + ".engines";

    public static String getSeriesBuilderClass(String engine) {
      return TIME_SERIES_ENGINE_CONFIG_PREFIX + "." + engine + ".series.builder.class";
    }
  }

  public static class BrokerConfigs {
    public static final String LOGICAL_PLANNER_CLASS_SUFFIX = "logical.planner.class";
  }
}
