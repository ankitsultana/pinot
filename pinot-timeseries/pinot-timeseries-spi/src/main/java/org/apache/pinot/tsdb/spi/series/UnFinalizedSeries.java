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


public class UnFinalizedSeries extends Series {
  private final Object[] _objectValues;

  public UnFinalizedSeries(String id, Long[] timeValues, TimeBuckets timeBuckets, Object[] values,
      List<String> tagNames, Object[] tagValues) {
    super(id, timeValues, timeBuckets, null, tagNames, tagValues);
    _objectValues = values;
  }

  public Double[] getValues() {
    throw new UnsupportedOperationException("Unfinalized series does not support getValues()");
  }

  public Object[] getObjectValues() {
    return _objectValues;
  }
}
