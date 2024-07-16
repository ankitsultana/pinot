package org.apache.pinot.tsdb.spi.plan;

import java.util.List;


public class ScanFilterAndProjectPlanNode extends BaseTimeSeriesPlanNode {
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
}
