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

import java.util.List;
import org.apache.pinot.tsdb.spi.TimeBuckets;


public abstract class BaseSeriesBuilder {
  protected final String _id;
  protected final TimeBuckets _timeBuckets;
  protected final List<String> _tagNames;
  protected final Object[] _tagValues;

  public BaseSeriesBuilder(String id, TimeBuckets timeBuckets, List<String> tagNames, Object[] tagValues) {
    _id = id;
    _timeBuckets = timeBuckets;
    _tagNames = tagNames;
    _tagValues = tagValues;
  }

  public abstract void addValueAtIndex(int timeBucketIndex, Double value);

  public void addValueAtIndex(int timeBucketIndex, String value) {
    throw new IllegalStateException("This aggregation function does not support string input");
  }

  public abstract void addValue(long timeValue, Double value);

  public void mergeSeries(Series series) {
    int numDataPoints = series.getTimeValues().length;
    for (int i = 0; i < numDataPoints; i++) {
      addValue(series.getTimeValues()[i], series.getValues()[i]);
    }
  }

  public void mergeAlignedSeries(Series series) {
    int numDataPoints = series.getTimeValues().length;
    for (int i = 0; i < numDataPoints; i++) {
      addValueAtIndex(i, series.getValues()[i]);
    }
  }

  public abstract Series build();
}