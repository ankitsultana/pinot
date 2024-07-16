package org.apache.pinot.tsdb.spi;

import java.time.Duration;
import java.util.Arrays;
import java.util.Objects;


public class TimeBuckets {
  private final Long[] _timeBuckets;
  private final Duration _bucketSize;

  private TimeBuckets(Long[] timeBuckets, Duration bucketSize) {
    if (bucketSize.toMinutes() < 1) {
      throw new IllegalArgumentException("Only allow processing data at a minimum of 1 minute granularity");
    }
    _timeBuckets = timeBuckets;
    _bucketSize = bucketSize;
  }

  public Long[] getTimeBuckets() {
    return _timeBuckets;
  }

  public Duration getBucketSize() {
    return _bucketSize;
  }

  public long getStartTime() {
    return _timeBuckets[0];
  }

  public long getEndTime() {
    return _timeBuckets[_timeBuckets.length - 1];
  }

  public long getRangeSeconds() {
    return _timeBuckets[_timeBuckets.length - 1] - _timeBuckets[0];
  }

  public int getNumBuckets() {
    return _timeBuckets.length;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof TimeBuckets)) {
      return false;
    }
    TimeBuckets other = (TimeBuckets) o;
    return this.getStartTime() == other.getStartTime() && this.getEndTime() == other.getEndTime()
        && this.getBucketSize().equals(other.getBucketSize());
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(_bucketSize);
    result = 31 * result + Arrays.hashCode(_timeBuckets);
    return result;
  }

  public static TimeBuckets of(long startTime, long endTime, Duration bucketSize) {
    if (isTimestampMillis(startTime)) {
      startTime /= 1000;
      endTime /= 1000;
    }
    return ofSeconds(startTime, endTime, bucketSize);
  }

  public static TimeBuckets ofSeconds(long startTimeSeconds, long endTimeSeconds, Duration bucketSize) {
    long stepSizeConverted = 60 * bucketSize.toMinutes();
    if ((endTimeSeconds - startTimeSeconds) % stepSizeConverted != 0) {
      throw new IllegalArgumentException("Time range is not divisible by step size");
    }
    Long[] timeBuckets = new Long[1 + (int) ((endTimeSeconds - startTimeSeconds) / stepSizeConverted)];
    for (int i = 0; i < timeBuckets.length; i++) {
      timeBuckets[i] = startTimeSeconds + i * stepSizeConverted;
    }
    if (timeBuckets[timeBuckets.length - 1] != endTimeSeconds) {
      throw new IllegalArgumentException("Bug in creating time buckets");
    }
    return new TimeBuckets(timeBuckets, bucketSize);
  }

  public static boolean isTimestampMillis(long timestamp) {
    // 946684800000 is 01 Jan 2000 00:00:00 GMT in Epoch Millis.
    // 946684800000 in epoch seconds is ~30k years in the future.
    // As long as the timestamp is after year 2000 and less than year ~30_000,
    // this function will return the correct result
    return timestamp >= 946684800000L;
  }

  public static long roundStartTime(long startTime, long stepSizeConverted) {
    startTime -= startTime % stepSizeConverted;
    return startTime;
  }

  public static long roundEndTime(long endTime, long stepSizeConverted) {
    endTime += (stepSizeConverted - (endTime % stepSizeConverted)) % stepSizeConverted;
    return endTime;
  }
}