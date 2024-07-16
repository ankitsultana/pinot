package org.apache.pinot.tsdb.spi.plan;

import java.util.List;


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
}
