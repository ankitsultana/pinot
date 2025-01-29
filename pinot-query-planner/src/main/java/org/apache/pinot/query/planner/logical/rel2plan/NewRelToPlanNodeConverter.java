package org.apache.pinot.query.planner.logical.rel2plan;

import com.google.common.collect.Sets;
import groovy.util.logging.Log;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.logical.LogicalWindow;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.pinot.calcite.rel.PinotDataDistribution;
import org.apache.pinot.calcite.rel.PinotDataDistribution.HashDistributionDesc;
import org.apache.pinot.calcite.rel.logical.PinotLogicalAggregate;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.common.utils.DatabaseUtils;
import org.apache.pinot.core.routing.RoutingManager;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.logical.RexExpressionUtils;
import org.apache.pinot.query.planner.plannode.FilterNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.plannode.ProjectNode;
import org.apache.pinot.query.planner.plannode.TableScanNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class NewRelToPlanNodeConverter {
  private static final Logger LOGGER = LoggerFactory.getLogger(NewRelToPlanNodeConverter.class);
  private static final int DEFAULT_STAGE_ID = -1;
  private LeafWorkerAssignment _leafWorkerAssignment;
  private long _requestId;

  public NewRelToPlanNodeConverter(RoutingManager routingManager, long requestId) {
    _leafWorkerAssignment = new LeafWorkerAssignment(routingManager);
    _requestId = requestId;
  }

  /**
   * Leaf-stage: ([agg] + [project] + [filter] + table scan). many more cases like lookup-join.
   * Step-0: [done] Wrap rel-nodes in WrappedRelNode.
   * Step-1: Identify leaf-stage cut-off ==> required for implicit exchange.
   * Step-2: [done] Assign workers to leaves.
   * Step-3: Assign workers, and add exchange to other nodes.
   *
   *
   * Step-3:
   * - for each node, 
   */
  public PlanNode toPlanNode(RelNode relNode) {
    PlanIdGenerator generator = new PlanIdGenerator();
    WrappedRelNode wrappedRelNode = WrappedRelNode.wrapRelTree(relNode, generator);
    Context context = new Context();
    context._pdd = _leafWorkerAssignment.compute(wrappedRelNode, _requestId);
    return null;
  }

  // done
  WrappedPlanNode handleTableScan(WrappedRelNode wrappedRelNode, Context context) {
    LogicalTableScan node = (LogicalTableScan) wrappedRelNode.getRelNode();
    // assumption: parallelism doesn't change.
    String tableName = getTableNameFromTableScan(node);
    List<RelDataTypeField> fields = node.getRowType().getFieldList();
    List<String> columns = new ArrayList<>(fields.size());
    for (RelDataTypeField field : fields) {
      columns.add(field.getName());
    }
    TableScanNode tableScanNode = new TableScanNode(DEFAULT_STAGE_ID, toDataSchema(node.getRowType()),
        PlanNode.NodeHint.fromRelHints(node.getHints()), convertInputs(node.getInputs()), tableName, columns);
    PinotDataDistribution leafDistribution = context._pdd.get(context._currentIndex);
    return new WrappedPlanNode(context.getNextPlanId(), tableScanNode, leafDistribution);
  }


  // done
  public WrappedPlanNode handleFilter(WrappedRelNode wrappedRelNode, WrappedPlanNode inputNode, Context context) {
    LogicalFilter node = (LogicalFilter) wrappedRelNode.getRelNode();
    // assumption: parallelism doesn't change.
    FilterNode filterNode = new FilterNode(DEFAULT_STAGE_ID, toDataSchema(node.getRowType()),
        PlanNode.NodeHint.fromRelHints(node.getHints()),  convertInputs(node.getInputs()),
        RexExpressionUtils.fromRexNode(node.getCondition()));
    return new WrappedPlanNode(context.getNextPlanId(), filterNode, inputNode.getPinotDataDistribution());
  }

  public WrappedPlanNode handleProject(LogicalProject node, WrappedPlanNode inputNode, Context context) {
    // assumption: parallelism doesn't change.
    List<RexExpression> rexExpressions = RexExpressionUtils.fromRexNodes(node.getProjects());
    ProjectNode projectNode = new ProjectNode(DEFAULT_STAGE_ID, toDataSchema(node.getRowType()),
        PlanNode.NodeHint.fromRelHints(node.getHints()),  convertInputs(node.getInputs()), rexExpressions);
    PinotDataDistribution inputDistribution = inputNode.getPinotDataDistribution();
    PinotDataDistribution.Type inputDistributionType = inputDistribution.getType();
    if (inputDistributionType != PinotDataDistribution.Type.HASH_PARTITIONED) {
      // Preserve distribution for anything except hash distribution
      return new WrappedPlanNode(context.getNextPlanId(), projectNode, inputDistribution);
    }
    for (HashDistributionDesc desc: inputDistribution.getHashDistributionDesc()) {
      // all of the key indexes should exist as RexInputRef
    }
  }

  private static DataSchema toDataSchema(RelDataType rowType) {
    if (rowType instanceof RelRecordType) {
      RelRecordType recordType = (RelRecordType) rowType;
      String[] columnNames = recordType.getFieldNames().toArray(new String[]{});
      ColumnDataType[] columnDataTypes = new ColumnDataType[columnNames.length];
      for (int i = 0; i < columnNames.length; i++) {
        columnDataTypes[i] = convertToColumnDataType(recordType.getFieldList().get(i).getType());
      }
      return new DataSchema(columnNames, columnDataTypes);
    } else {
      throw new IllegalArgumentException("Unsupported RelDataType: " + rowType);
    }
  }

  private List<PlanNode> convertInputs(List<WrappedRelNode> inputs) {
    // NOTE: Inputs can be modified in place. Do not create immutable List here.
    int numInputs = inputs.size();
    List<PlanNode> planNodes = new ArrayList<>(numInputs);
    for (RelNode input : inputs) {
      planNodes.add(toPlanNode(input));
    }
    return planNodes;
  }

  public static String getTableNameFromTableScan(TableScan tableScan) {
    return getTableNameFromRelTable(tableScan.getTable());
  }

  public static String getTableNameFromRelTable(RelOptTable table) {
    List<String> qualifiedName = table.getQualifiedName();
    return qualifiedName.size() == 1 ? qualifiedName.get(0)
        : DatabaseUtils.constructFullyQualifiedTableName(qualifiedName.get(0), qualifiedName.get(1));
  }

  public static ColumnDataType convertToColumnDataType(RelDataType relDataType) {
    SqlTypeName sqlTypeName = relDataType.getSqlTypeName();
    if (sqlTypeName == SqlTypeName.NULL) {
      return ColumnDataType.UNKNOWN;
    }
    boolean isArray = (sqlTypeName == SqlTypeName.ARRAY);
    if (isArray) {
      assert relDataType.getComponentType() != null;
      sqlTypeName = relDataType.getComponentType().getSqlTypeName();
    }
    switch (sqlTypeName) {
      case BOOLEAN:
        return isArray ? ColumnDataType.BOOLEAN_ARRAY : ColumnDataType.BOOLEAN;
      case TINYINT:
      case SMALLINT:
      case INTEGER:
        return isArray ? ColumnDataType.INT_ARRAY : ColumnDataType.INT;
      case BIGINT:
        return isArray ? ColumnDataType.LONG_ARRAY : ColumnDataType.LONG;
      case DECIMAL:
        return resolveDecimal(relDataType, isArray);
      case FLOAT:
      case REAL:
        return isArray ? ColumnDataType.FLOAT_ARRAY : ColumnDataType.FLOAT;
      case DOUBLE:
        return isArray ? ColumnDataType.DOUBLE_ARRAY : ColumnDataType.DOUBLE;
      case DATE:
      case TIME:
      case TIMESTAMP:
        return isArray ? ColumnDataType.TIMESTAMP_ARRAY : ColumnDataType.TIMESTAMP;
      case CHAR:
      case VARCHAR:
        return isArray ? ColumnDataType.STRING_ARRAY : ColumnDataType.STRING;
      case BINARY:
      case VARBINARY:
        return isArray ? ColumnDataType.BYTES_ARRAY : ColumnDataType.BYTES;
      case MAP:
        return ColumnDataType.MAP;
      case OTHER:
      case ANY:
        return ColumnDataType.OBJECT;
      default:
        if (relDataType.getComponentType() != null) {
          throw new IllegalArgumentException("Unsupported collection type: " + relDataType);
        }
        LOGGER.warn("Unexpected SQL type: {}, use OBJECT instead", sqlTypeName);
        return ColumnDataType.OBJECT;
    }
  }

  /**
   * Calcite uses DEMICAL type to infer data type hoisting and infer arithmetic result types. down casting this back to
   * the proper primitive type for Pinot.
   * TODO: Revisit this method:
   *  - Currently we are converting exact value to approximate value
   *  - Integer can only cover all values with precision 9; Long can only cover all values with precision 18
   *
   * @param relDataType the DECIMAL rel data type.
   * @param isArray
   * @return proper {@link ColumnDataType}.
   * @see {@link org.apache.calcite.rel.type.RelDataTypeFactoryImpl#decimalOf}.
   */
  private static ColumnDataType resolveDecimal(RelDataType relDataType, boolean isArray) {
    int precision = relDataType.getPrecision();
    int scale = relDataType.getScale();
    if (scale == 0) {
      if (precision <= 10) {
        return isArray ? ColumnDataType.INT_ARRAY : ColumnDataType.INT;
      } else if (precision <= 38) {
        return isArray ? ColumnDataType.LONG_ARRAY : ColumnDataType.LONG;
      } else {
        return isArray ? ColumnDataType.DOUBLE_ARRAY : ColumnDataType.BIG_DECIMAL;
      }
    } else {
      // NOTE: Do not use FLOAT to represent DECIMAL to be consistent with single-stage engine behavior.
      //       See {@link RequestUtils#getLiteralExpression(SqlLiteral)}.
      if (precision <= 30) {
        return isArray ? ColumnDataType.DOUBLE_ARRAY : ColumnDataType.DOUBLE;
      } else {
        return isArray ? ColumnDataType.DOUBLE_ARRAY : ColumnDataType.BIG_DECIMAL;
      }
    }
  }

  static class Context {
    int _currentIndex = 0;
    Map<Integer, PinotDataDistribution> _pdd;
    PlanIdGenerator _planIdGenerator;

    long getNextPlanId() {
      return _planIdGenerator.get();
    }
  }
}
