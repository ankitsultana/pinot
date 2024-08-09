package org.apache.pinot.tsdb.spi.plan;

import java.util.List;
import org.apache.pinot.tsdb.spi.operator.BaseTimeSeriesOperator;
import org.apache.pinot.tsdb.spi.plan.visitor.PlanVisitor;
import org.apache.pinot.tsdb.spi.time.QueryTimeBoundaryConstraints;


public abstract class BaseTimeSeriesPlanNode {
  protected final String _id;
  protected final List<BaseTimeSeriesPlanNode> _children;

  public BaseTimeSeriesPlanNode(String id, List<BaseTimeSeriesPlanNode> children) {
    _id = id;
    _children = children;
  }

  public String getId() {
    return _id;
  }

  public List<BaseTimeSeriesPlanNode> getChildren() {
    return _children;
  }

  public void addChildNode(BaseTimeSeriesPlanNode planNode) {
    _children.add(planNode);
  }

  public QueryTimeBoundaryConstraints process(QueryTimeBoundaryConstraints timeBoundary) {
    return timeBoundary;
  }

  public abstract String getKlass();

  public abstract String getExplainName();

  public abstract <T> T accept(PlanVisitor<T> visitor);

  public abstract BaseTimeSeriesOperator run();
}
