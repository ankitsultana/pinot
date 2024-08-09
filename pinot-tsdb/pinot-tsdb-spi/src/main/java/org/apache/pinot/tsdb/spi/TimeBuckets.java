package org.apache.pinot.tsdb.spi;

import java.time.Duration;
import java.util.Arrays;
import java.util.Objects;


/**
 * Time buckets used for query execution. Each element (say x) in the {@link #getTimeBuckets()} array represents a
 * time-range which is half open on the right side: [x, x + bucketSize.getSeconds()). Some query languages allow some
 * operators to mutate the time-buckets on the fly, so it is not guaranteed that you will have a single time-bucket
 * across the entire query execution.
 */
public class TimeBuckets {
  private final Long[] _timeBuckets;
  private final Duration _bucketSize;

  private TimeBuckets(Long[] timeBuckets, Duration bucketSize) {
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

  public int resolveIndex(long timeValue) {
    if (_timeBuckets.length == 0) {
      return -1;
    }
    if (timeValue < _timeBuckets[0]) {
      return -1;
    }
    if (timeValue >= _timeBuckets[_timeBuckets.length - 1] + _bucketSize.getSeconds()) {
      return -1;
    }
    return (int) ((timeValue - _timeBuckets[0]) / _bucketSize.getSeconds());
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

  public static TimeBuckets ofSeconds(long startTimeSeconds, Duration bucketSize, int numElements) {
    long stepSize = bucketSize.getSeconds();
    Long[] timeBuckets = new Long[numElements];
    for (int i = 0; i < numElements; i++) {
      timeBuckets[i] = startTimeSeconds + i * stepSize;
    }
    return new TimeBuckets(timeBuckets, bucketSize);
  }
}
