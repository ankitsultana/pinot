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
package org.apache.pinot.tsdb.spi.series.builders;

import java.util.List;
import org.apache.pinot.tsdb.spi.TimeBuckets;
import org.apache.pinot.tsdb.spi.series.BaseSeriesBuilder;
import org.apache.pinot.tsdb.spi.series.Series;


public class MinSeriesBuilder extends BaseSeriesBuilder {
  private final Double[] _values;

  public MinSeriesBuilder(String id, TimeBuckets timeBuckets, List<String> tagNames, Object[] tagValues) {
    super(id, timeBuckets, tagNames, tagValues);
    _values = new Double[timeBuckets.getNumBuckets()];
  }

  @Override
  public void addValueAtIndex(int timeBucketIndex, Double value) {
    if (_values[timeBucketIndex] == null || value < _values[timeBucketIndex]) {
      _values[timeBucketIndex] = value;
    }
  }

  @Override
  public void addValue(long timeValue, Double value) {
    int timeBucketIndex = _timeBuckets.resolveIndex(timeValue);
    addValueAtIndex(timeBucketIndex, value);
  }

  @Override
  public Series build() {
    return new Series(_id, null, _timeBuckets, _values, _tagNames, _tagValues);
  }
}
