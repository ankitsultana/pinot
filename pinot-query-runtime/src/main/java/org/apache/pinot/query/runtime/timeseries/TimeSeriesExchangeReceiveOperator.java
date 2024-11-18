package org.apache.pinot.query.runtime.timeseries;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.pinot.tsdb.spi.AggInfo;
import org.apache.pinot.tsdb.spi.TimeBuckets;
import org.apache.pinot.tsdb.spi.operator.BaseTimeSeriesOperator;
import org.apache.pinot.tsdb.spi.series.BaseTimeSeriesBuilder;
import org.apache.pinot.tsdb.spi.series.TimeSeries;
import org.apache.pinot.tsdb.spi.series.TimeSeriesBlock;
import org.apache.pinot.tsdb.spi.series.TimeSeriesBuilderFactory;


public class TimeSeriesExchangeReceiveOperator extends BaseTimeSeriesOperator {
  private final BlockingQueue<Object> _receiver;
  private final long _deadlineMs;
  private final int _expectedBlocks;
  @Nullable
  private final AggInfo _aggInfo;
  private final TimeSeriesBuilderFactory _factory;

  public TimeSeriesExchangeReceiveOperator(BlockingQueue<Object> receiver, long deadlineMs, int expectedBlocks,
      @Nullable AggInfo aggInfo, TimeSeriesBuilderFactory seriesBuilderFactory) {
    super(Collections.emptyList());
    _receiver = receiver;
    _deadlineMs = deadlineMs;
    _expectedBlocks = expectedBlocks;
    _aggInfo = aggInfo;
    _factory = seriesBuilderFactory;
  }

  @Override
  public TimeSeriesBlock getNextBlock() {
    try {
      if (_aggInfo == null) {
        return getNextBlockNoAggregation();
      } else {
        return getNextBlockWithAggregation();
      }
    } catch (Exception e) {
      throw new RuntimeException("Error in Time Series Exchange Receive Operator", e);
    }
  }

  private TimeSeriesBlock getNextBlockWithAggregation() {
    try {
      TimeBuckets timeBuckets = null;
      Map<Long, BaseTimeSeriesBuilder> seriesBuilderMap = new HashMap<>();
      for (int index = 0; index < _expectedBlocks; index++) {
        long remainingTimeMs = _deadlineMs - System.currentTimeMillis();
        Preconditions.checkState(remainingTimeMs > 0, "Timed out before polling exchange receive");
        Object result = _receiver.poll(remainingTimeMs, TimeUnit.MILLISECONDS);
        Preconditions.checkNotNull(result, "Timed out waiting for response. Waited: %s ms", remainingTimeMs);
        if (result instanceof Throwable) {
          throw new RuntimeException("Received error in exchange receive", (Throwable) result);
        }
        Preconditions.checkState(result instanceof TimeSeriesBlock, "Found unexpected object. This is a bug: %s", result.getClass());
        TimeSeriesBlock blockToMerge = (TimeSeriesBlock) result;
        if (timeBuckets == null) {
          timeBuckets = blockToMerge.getTimeBuckets();
        } else {
          Preconditions.checkState(timeBuckets.equals(blockToMerge.getTimeBuckets()),
              "Found unequal time buckets from server response");
        }
        for (var entry : blockToMerge.getSeriesMap().entrySet()) {
          long seriesHash = entry.getKey();
          Preconditions.checkState(entry.getValue().size() == 1,
              "Expected exactly 1 time series in server response, found: %s", entry.getValue().size());
          TimeSeries currentTimeSeries = entry.getValue().get(0);
          BaseTimeSeriesBuilder seriesBuilder = seriesBuilderMap.get(seriesHash);
          if (seriesBuilder == null) {
            seriesBuilder = _factory.newTimeSeriesBuilder(
                _aggInfo, Long.toString(seriesHash), timeBuckets, currentTimeSeries.getTagNames(),
                currentTimeSeries.getTagValues());
            seriesBuilderMap.put(seriesHash, seriesBuilder);
          }
          seriesBuilder.mergeAlignedSeries(currentTimeSeries);
        }
      }
      Map<Long, List<TimeSeries>> seriesMap = new HashMap<>(seriesBuilderMap.size());
      for (var entry : seriesBuilderMap.entrySet()) {
        long seriesHash = entry.getKey();
        List<TimeSeries> timeSeriesList = new ArrayList<>();
        timeSeriesList.add(entry.getValue().build());
        seriesMap.put(seriesHash, timeSeriesList);
      }
      return new TimeSeriesBlock(timeBuckets, seriesMap);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private TimeSeriesBlock getNextBlockNoAggregation()
      throws InterruptedException {
    Map<Long, List<TimeSeries>> timeSeriesMap = new HashMap<>();
    TimeBuckets timeBuckets = null;
    for (int index = 0; index < _expectedBlocks; index++) {
      long remainingTimeMs = _deadlineMs - System.currentTimeMillis();
      Preconditions.checkState(remainingTimeMs > 0, "Timed out before polling exchange receive");
      Object result = _receiver.poll(remainingTimeMs, TimeUnit.MILLISECONDS);
      Preconditions.checkNotNull(result, "Timed out waiting for response. Waited: %s ms", remainingTimeMs);
      if (result instanceof Throwable) {
        throw new RuntimeException("Received error in exchange receive", (Throwable) result);
      }
      Preconditions.checkState(result instanceof TimeSeriesBlock, "Found unexpected object. This is a bug: %s", result.getClass());
      TimeSeriesBlock blockToMerge = (TimeSeriesBlock) result;
      if (timeBuckets == null) {
        timeBuckets = blockToMerge.getTimeBuckets();
      } else {
        Preconditions.checkState(timeBuckets.equals(blockToMerge.getTimeBuckets()),
            "Found unequal time buckets from server response");
      }
      for (var entry : blockToMerge.getSeriesMap().entrySet()) {
        long seriesHash = entry.getKey();
        Preconditions.checkState(!timeSeriesMap.containsKey(seriesHash), "Broker no-aggregation receive"
            + " cannot receive the same series multiple times");
        List<TimeSeries> timeSeriesList = new ArrayList<>(entry.getValue());
        timeSeriesMap.put(seriesHash, timeSeriesList);
      }
    }
    return new TimeSeriesBlock(timeBuckets, timeSeriesMap);
  }

  @Override
  public String getExplainName() {
    return "TIME_SERIES_EXCHANGE_RECEIVE";
  }
}
