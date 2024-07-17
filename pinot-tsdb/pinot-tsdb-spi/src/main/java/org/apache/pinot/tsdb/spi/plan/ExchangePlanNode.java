package org.apache.pinot.tsdb.spi.plan;

import com.fasterxml.jackson.annotation.JsonCreator;
import java.util.List;
import org.apache.pinot.tsdb.spi.operator.BaseTimeSeriesOperator;
import org.apache.pinot.tsdb.spi.plan.visitor.PlanVisitor;


public class ExchangePlanNode extends BaseTimeSeriesPlanNode {
  private static final String EXPLAIN_NAME = "EXCHANGE";

  @JsonCreator
  public ExchangePlanNode(String id, List<BaseTimeSeriesPlanNode> children) {
    super(id, children);
  }

  @Override
  public String getKlass() {
    return ExchangePlanNode.class.getName();
  }

  @Override
  public String getExplainName() {
    return EXPLAIN_NAME;
  }

  @Override
  public <T> T accept(PlanVisitor<T> visitor) {
    return visitor.visit(this);
  }

  @Override
  public BaseTimeSeriesOperator run() {
    throw new UnsupportedOperationException("Exchange not supported yet");
  }
}
