package org.apache.pinot.tsdb.spi.plan;

import java.util.List;
import org.apache.pinot.tsdb.spi.operator.BaseTimeSeriesOperator;
import org.apache.pinot.tsdb.spi.plan.visitor.PlanVisitor;


public class ScanFilterAndProjectPlanNode extends BaseTimeSeriesPlanNode {
  private static final String EXPLAIN_NAME = "SCAN_FILTER_AND_PROJECT";
  private final String _tableName;
  private final String _filterExpression;
  private final String _valueExpression;
  private final List<String> _projects;

  public ScanFilterAndProjectPlanNode(
      String id, List<BaseTimeSeriesPlanNode> children,
      String tableName, String filterExpression, String valueExpression, List<String> projects) {
    super(id, children);
    _tableName = tableName;
    _filterExpression = filterExpression;
    _valueExpression = valueExpression;
    _projects = projects;
  }

  @Override
  public String getKlass() {
    return ScanFilterAndProjectPlanNode.class.getName();
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
    throw new UnsupportedOperationException("");
  }

  public String getTableName() {
    return _tableName;
  }

  public String getFilterExpression() {
    return _filterExpression;
  }

  public String getValueExpression() {
    return _valueExpression;
  }

  public List<String> getProjects() {
    return _projects;
  }
}
