package org.apache.pinot.tsdb.spi.plan;

import java.util.List;


public class ExchangePlanNode extends BaseTimeSeriesPlanNode {
  public ExchangePlanNode(String id, List<BaseTimeSeriesPlanNode> children) {
    super(id, children);
  }
}
