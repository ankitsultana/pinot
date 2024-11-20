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
package org.apache.pinot.query.runtime.timeseries.serde;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.tsdb.spi.TimeBuckets;
import org.apache.pinot.tsdb.spi.series.TimeSeries;
import org.apache.pinot.tsdb.spi.series.TimeSeriesBlock;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class TimeSeriesBlockSerdeTest {
  @Test
  public void testSerde()
      throws IOException {
    TimeSeriesBlock block = buildBlock();
    ByteString byteString = TimeSeriesBlockSerde.serializeTimeSeriesBlock(block);
    String firstString = byteString.toStringUtf8();
    TimeSeriesBlock deserializedBlock = TimeSeriesBlockSerde.deserializeTimeSeriesBlock(byteString.asReadOnlyByteBuffer());
    assertNotNull(deserializedBlock);
    String secondString = TimeSeriesBlockSerde.serializeTimeSeriesBlock(deserializedBlock).toStringUtf8();
    assertEquals(firstString, secondString);
  }

  private static TimeSeriesBlock buildBlock() {
    TimeBuckets timeBuckets = TimeBuckets.ofSeconds(1000, Duration.ofSeconds(200), 5);
    List<String> tagNames = ImmutableList.of("cityId");
    Map<Long, List<TimeSeries>> seriesMap = new HashMap<>();
    seriesMap.put(123L, ImmutableList.of(new TimeSeries("123", null, timeBuckets,
        new Double[]{null, 123.0, 0.0, 1.0}, tagNames, new Object[]{"Chicago"})));
    seriesMap.put(244L, ImmutableList.of(new TimeSeries("244", null, timeBuckets,
        new Double[]{null, null, null, null}, tagNames, new Object[]{"San Francisco"})));
    return new TimeSeriesBlock(timeBuckets, seriesMap);
  }
}
