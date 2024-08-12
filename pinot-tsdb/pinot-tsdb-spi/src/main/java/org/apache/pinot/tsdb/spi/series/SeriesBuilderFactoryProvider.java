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
