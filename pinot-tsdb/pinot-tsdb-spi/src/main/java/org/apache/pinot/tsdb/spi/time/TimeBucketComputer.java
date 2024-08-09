package org.apache.pinot.tsdb.spi.time;

import org.apache.pinot.tsdb.spi.RangeTimeSeriesRequest;
import org.apache.pinot.tsdb.spi.TimeBuckets;
import org.apache.pinot.tsdb.spi.plan.BaseTimeSeriesPlanNode;


public class TimeBucketComputer {
  private final RangeTimeSeriesRequest _rangeTimeSeriesRequest;

  public TimeBucketComputer(RangeTimeSeriesRequest rangeTimeSeriesRequest) {
    _rangeTimeSeriesRequest = rangeTimeSeriesRequest;
  }

  public TimeBuckets compute(BaseTimeSeriesPlanNode planNode) {
    QueryTimeBoundaryConstraints constraints = computeInternal(planNode);
  }

  public static TimeBuckets computeTimeBuckets(RangeTimeSeriesRequest rangeTimeSeriesRequest,
      QueryTimeBoundaryConstraints constraints) {
    if (constraints.isLeftAligned()) {
      throw new IllegalArgumentException("Don't support left aligned buckets yet");
    }
  }

  private QueryTimeBoundaryConstraints computeInternal(BaseTimeSeriesPlanNode planNode) {
    if (planNode.getChildren().isEmpty()) {
      return planNode.process(createDefault());
    }
    QueryTimeBoundaryConstraints queryTimeBoundary = null;
    for (BaseTimeSeriesPlanNode child : planNode.getChildren()) {
      QueryTimeBoundaryConstraints childQueryTimeBoundary = computeInternal(child);
      if (queryTimeBoundary == null) {
        queryTimeBoundary = childQueryTimeBoundary;
      } else {
        queryTimeBoundary = QueryTimeBoundaryConstraints.merge(queryTimeBoundary, childQueryTimeBoundary);
      }
    }
    return planNode.process(queryTimeBoundary);
  }

  private QueryTimeBoundaryConstraints createDefault() {
    QueryTimeBoundaryConstraints queryTimeBoundary = new QueryTimeBoundaryConstraints();
    queryTimeBoundary.getDivisors().add(_rangeTimeSeriesRequest.getStepSeconds());
    return queryTimeBoundary;
  }
}
