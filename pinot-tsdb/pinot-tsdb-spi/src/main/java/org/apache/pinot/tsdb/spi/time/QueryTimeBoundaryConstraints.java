package org.apache.pinot.tsdb.spi.time;

import java.util.HashSet;
import java.util.Set;
import org.apache.pinot.tsdb.spi.TimeBuckets;
import org.apache.pinot.tsdb.spi.plan.BaseTimeSeriesPlanNode;


/**
 * This can be used by each time-series query language to influence the computed TimeBuckets. Each plan
 * node's {@link BaseTimeSeriesPlanNode#process(QueryTimeBoundaryConstraints)} will be called in a
 * post-order traversal of the plan tree. The plan node can thus modify the constraints as needed.
 * <p>
 *   The {@link TimeBuckets} are computed by the {@link TimeBucketComputer} after taking the computed
 *   constraints into account. Moreover, the {@link TimeBuckets} hence computed are the default buckets
 *   used for the table-scan stage of the query execution. Some query languages allow users to change
 *   the bucket resolution on the fly (e.g. M3QL allows "summarize"), hence it's not guaranteed that
 *   the resulting time-series of the query will have the same time-values as the {@link TimeBuckets}
 *   computed by the {@link TimeBucketComputer}.
 * </p>
 * <b>Use-Cases</b>
 * <ul>
 *   <li>Extend Time Buckets in Either Direction</li>
 *   <li>Extend Time Buckets to Meet Divisibility</li>
 *   <li>Change Alignment</li>
 *   <li>Hint for Time Range Filters</li>
 * </ul>
 */
public class QueryTimeBoundaryConstraints {
  private final Set<Long> _divisors = new HashSet<>();
  private long _leftExtensionSeconds = 0;
  private long _rightExtensionSeconds = 0;
  private boolean _leftAligned = false;

  public Set<Long> getDivisors() {
    return _divisors;
  }

  public long getLeftExtensionSeconds() {
    return _leftExtensionSeconds;
  }

  public void setLeftExtensionSeconds(long leftExtensionSeconds) {
    _leftExtensionSeconds = leftExtensionSeconds;
  }

  public long getRightExtensionSeconds() {
    return _rightExtensionSeconds;
  }

  public void setRightExtensionSeconds(long rightExtensionSeconds) {
    _rightExtensionSeconds = rightExtensionSeconds;
  }

  public boolean isLeftAligned() {
    return _leftAligned;
  }

  public void setLeftAligned(boolean leftAligned) {
    _leftAligned = leftAligned;
  }

  public static QueryTimeBoundaryConstraints merge(QueryTimeBoundaryConstraints left, QueryTimeBoundaryConstraints right) {
    QueryTimeBoundaryConstraints merged = new QueryTimeBoundaryConstraints();
    merged._divisors.addAll(left._divisors);
    merged._divisors.addAll(right._divisors);
    merged._leftExtensionSeconds = Math.max(left._leftExtensionSeconds, right._leftExtensionSeconds);
    merged._rightExtensionSeconds = Math.max(left._rightExtensionSeconds, right._rightExtensionSeconds);
    if (left._leftAligned != right._leftAligned) {
      throw new IllegalArgumentException(String.format("Cannot merge constraints with different alignments. "
          + "Alignment from plan node on the left and right are %s and %s respectively",
          left._leftAligned, right._leftAligned));
    }
    merged._leftAligned = left._leftAligned;
    return merged;
  }
}
