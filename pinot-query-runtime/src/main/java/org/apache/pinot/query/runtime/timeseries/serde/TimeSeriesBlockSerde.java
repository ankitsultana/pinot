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

import com.google.protobuf.ByteString;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.datablock.DataBlockUtils;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.apache.pinot.tsdb.spi.TimeBuckets;
import org.apache.pinot.tsdb.spi.series.TimeSeries;
import org.apache.pinot.tsdb.spi.series.TimeSeriesBlock;


/**
 * Implements a simple Serde mechanism for Time Series Block.
 * TODO: One source of inefficiency is boxing/unboxing of Double arrays.
 * TODO: The other is tag values being Object[]. We should make tag values String[].
 */
public class TimeSeriesBlockSerde {
  private static final DataSchema DATA_SCHEMA;

  static {
    DATA_SCHEMA = new DataSchema(new String[]{"longColumn", "tagNames", "tagValues", "doubleValues"},
        new ColumnDataType[]{ColumnDataType.LONG, ColumnDataType.STRING_ARRAY, ColumnDataType.STRING_ARRAY, ColumnDataType.DOUBLE_ARRAY});
  }

  private TimeSeriesBlockSerde() {
  }

  public static TimeSeriesBlock deserializeTimeSeriesBlock(ByteBuffer readOnlyByteBuffer)
      throws IOException {
    DataBlock dataBlock = DataBlockUtils.readFrom(readOnlyByteBuffer);
    TransferableBlock transferableBlock = TransferableBlockUtils.wrap(dataBlock);
    List<Object[]> container = transferableBlock.getContainer();
    TimeBuckets timeBuckets = toTimeBuckets(container.get(0));
    Map<Long, List<TimeSeries>> seriesMap = new HashMap<>();
    for (int index = 1; index < container.size(); index++) {
      Object[] row = container.get(index);
      long seriesId = Long.parseLong(row[0].toString());
      seriesMap.computeIfAbsent(seriesId, (x) -> new ArrayList<>()).add(toTimeSeries(row, timeBuckets));
    }
    return new TimeSeriesBlock(timeBuckets, seriesMap);
  }

  public static ByteString serializeTimeSeriesBlock(TimeSeriesBlock timeSeriesBlock)
      throws IOException {
    TimeBuckets timeBuckets = Objects.requireNonNull(timeSeriesBlock.getTimeBuckets());
    List<Object[]> container = new ArrayList<>();
    container.add(fromTimeBuckets(timeBuckets));
    for (var entry : timeSeriesBlock.getSeriesMap().entrySet()) {
      for (TimeSeries timeSeries : entry.getValue()) {
        container.add(fromTimeSeries(timeSeries));
      }
    }
    TransferableBlock transferableBlock = new TransferableBlock(container, DATA_SCHEMA, DataBlock.Type.ROW);
    return DataBlockUtils.toByteString(transferableBlock.getDataBlock());
  }

  private static Object[] fromTimeBuckets(TimeBuckets timeBuckets) {
    Object[] result = new Object[4];
    result[0] = timeBuckets.getTimeBuckets()[0];
    result[1] = new String[]{Long.toString(timeBuckets.getBucketSize().getSeconds())};
    result[2] = new String[]{};
    result[3] = new double[]{(double) timeBuckets.getNumBuckets()};
    return result;
  }

  private static TimeBuckets toTimeBuckets(Object[] objects) {
    long fbv = Long.parseLong(objects[0].toString());
    Duration window = Duration.ofSeconds(Long.parseLong(((String[]) objects[1])[0]));
    int numBuckets = (int) ((double[]) objects[3])[0];
    return TimeBuckets.ofSeconds(fbv, window, numBuckets);
  }

  private static Object[] fromTimeSeries(TimeSeries timeSeries) {
    Object[] result = new Object[4];
    result[0] = Long.parseLong(timeSeries.getId());
    result[1] = fromTagNamesList(timeSeries.getTagNames());
    result[2] = fromTagValuesList(timeSeries.getTagValues());
    result[3] = unboxDoubleArray(timeSeries.getValues());
    return result;
  }

  private static TimeSeries toTimeSeries(Object[] row, TimeBuckets timeBuckets) {
    Long seriesId = (Long) row[0];
    List<String> tagNames = List.of((String[]) row[1]);
    Double[] values = boxDoubleArray((double[]) row[3]);
    Object[] tagValues = (Object[]) row[2];
    return new TimeSeries(Long.toString(seriesId), null, timeBuckets, values, tagNames, tagValues);
  }

  private static String[] fromTagNamesList(List<String> tagNames) {
    String[] result = new String[tagNames.size()];
    for (int index = 0; index < result.length; index++) {
      result[index] = tagNames.get(index);
    }
    return result;
  }

  private static String[] fromTagValuesList(Object[] values) {
    String[] result = new String[values.length];
    for (int index = 0; index < result.length; index++) {
      result[index] = values[index] == null ? "null" : values[index].toString();
    }
    return result;
  }

  private static double[] unboxDoubleArray(Double[] values) {
    double[] result = new double[values.length];
    for (int index = 0; index < result.length; index++) {
      result[index] = values[index] == null ? Double.MIN_VALUE : values[index];
    }
    return result;
  }

  private static Double[] boxDoubleArray(double[] values) {
    Double[] result = new Double[values.length];
    for (int index = 0; index < result.length; index++) {
      result[index] = values[index] == Double.MIN_VALUE ? null : values[index];
    }
    return result;
  }
}
