/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.core.operator.combine;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.commons.lang.ArrayUtils;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.common.RowBasedBlockValueFetcher;
import org.apache.pinot.core.operator.ExecutionStatistics;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;
import org.apache.pinot.core.operator.blocks.TransformBlock;
import org.apache.pinot.core.operator.transform.TransformOperator;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.selection.SelectionOperatorUtils;
import org.apache.pinot.spi.data.FieldSpec;
import org.roaringbitmap.RoaringBitmap;


public class LocalJoinOperator extends BaseCombineOperator {
  private final List<TransformOperator> _leftOperators;
  private final List<TransformOperator> _rightOperators;
  private final List<ExpressionContext> _leftExpressions;
  private final List<ExpressionContext> _rightExpressions;
  private final BlockValSet[] _rightBlockValSets;
  private final BlockValSet[] _leftBlockValSets;
  private final boolean _nullHandlingEnabled;
  private final int _numRowsToKeep;
  private final List<Object[]> _rightRows;
  private final RoaringBitmap[] _leftNullBitmaps;
  private final RoaringBitmap[] _rightNullBitmaps;
  private final int _leftJoinIndex;
  private final int _rightJoinIndex;
  private int _finalNumColumns;
  private DataSchema _dataSchema;
  private final boolean _reorder;

  public LocalJoinOperator(List<TransformOperator> leftOperators,
      List<TransformOperator> rightOperators, Collection<ExpressionContext> leftExpressions,
      Collection<ExpressionContext> rightExpressions, QueryContext queryContext, ExecutorService executorService,
      boolean reorder) {
    super(Collections.emptyList(), queryContext, executorService);
    _leftOperators = leftOperators;
    _rightOperators = rightOperators;
    _leftExpressions = new ArrayList<>(leftExpressions);
    _rightExpressions = new ArrayList<>(rightExpressions);

    _leftBlockValSets = new BlockValSet[_leftExpressions.size()];
    _rightBlockValSets = new BlockValSet[_rightExpressions.size()];
    _nullHandlingEnabled = queryContext.isNullHandlingEnabled();
    _numRowsToKeep = queryContext.getLimit();
    _rightRows = new ArrayList<>(Math.min(_numRowsToKeep, SelectionOperatorUtils.MAX_ROW_HOLDER_INITIAL_CAPACITY));
    _leftNullBitmaps = _nullHandlingEnabled ? new RoaringBitmap[_leftExpressions.size()] : null;
    _rightNullBitmaps = _nullHandlingEnabled ? new RoaringBitmap[_rightExpressions.size()] : null;
    Preconditions.checkNotNull(queryContext.getLeftJoinColumnIndices());
    _leftJoinIndex = queryContext.getLeftJoinColumnIndices().get(0);
    Preconditions.checkNotNull(queryContext.getRightJoinColumnIndices());
    _rightJoinIndex = queryContext.getRightJoinColumnIndices().get(0);
    _reorder = reorder;
    computeDataSchema();
  }

  @Override
  public List<Operator> getChildOperators() {
    return null;
  }

  @Nullable
  @Override
  public String toExplainString() {
    return null;
  }

  @Override
  protected IntermediateResultsBlock getNextBlock() {
    TransformBlock transformBlock;
    for (Operator<TransformBlock> transformOperator : _rightOperators) {
      while ((transformBlock = transformOperator.nextBlock()) != null) {
        int numExpressions = _rightExpressions.size();
        for (int i = 0; i < numExpressions; i++) {
          _rightBlockValSets[i] = transformBlock.getBlockValueSet(_rightExpressions.get(i));
        }
        RowBasedBlockValueFetcher blockValueFetcher = new RowBasedBlockValueFetcher(_rightBlockValSets);

        int numDocsToAdd = transformBlock.getNumDocs();
        if (_nullHandlingEnabled) {
          for (int i = 0; i < numExpressions; i++) {
            _rightNullBitmaps[i] = _rightBlockValSets[i].getNullBitmap();
          }
          for (int docId = 0; docId < numDocsToAdd; docId++) {
            Object[] values = blockValueFetcher.getRow(docId);
            for (int colId = 0; colId < numExpressions; colId++) {
              if (_rightNullBitmaps[colId] != null && _rightNullBitmaps[colId].contains(docId)) {
                values[colId] = null;
              }
            }
            _rightRows.add(values);
          }
        } else {
          for (int i = 0; i < numDocsToAdd; i++) {
            _rightRows.add(blockValueFetcher.getRow(i));
          }
        }
        if (_rightRows.size() == _numRowsToKeep) {
          break;
        }
      }
    }
    Preconditions.checkNotNull(_queryContext.getLeftJoinColumnIndices());
    Preconditions.checkNotNull(_queryContext.getRightJoinColumnIndices());
    int rightIndex = _queryContext.getRightJoinColumnIndices().get(0);
    Map<Object, List<Object[]>> rightTable = new HashMap<>();
    for (Object[] row : _rightRows) {
      rightTable.computeIfAbsent(row[rightIndex], (x) -> new ArrayList<>());
      rightTable.get(row[rightIndex]).add(row);
    }
    List<Object[]> joinedRows = new ArrayList<>();
    for (Operator<TransformBlock> transformOperator : _leftOperators) {
      while ((transformBlock = transformOperator.nextBlock()) != null) {
        int numExpressions = _leftExpressions.size();
        for (int i = 0; i < numExpressions; i++) {
          _leftBlockValSets[i] = transformBlock.getBlockValueSet(_leftExpressions.get(i));
        }
        RowBasedBlockValueFetcher blockValueFetcher = new RowBasedBlockValueFetcher(_leftBlockValSets);

        int numDocsToExplore = transformBlock.getNumDocs();
        if (_nullHandlingEnabled) {
          for (int i = 0; i < numExpressions; i++) {
            _leftNullBitmaps[i] = _leftBlockValSets[i].getNullBitmap();
          }
          for (int docId = 0; docId < numDocsToExplore; docId++) {
            Object[] values = blockValueFetcher.getRow(docId);
            for (int colId = 0; colId < numExpressions; colId++) {
              if (_leftNullBitmaps[colId] != null && _leftNullBitmaps[colId].contains(docId)) {
                values[colId] = null;
              }
            }
            evaluateJoin(values, rightTable, joinedRows);
          }
        } else {
          for (int i = 0; i < numDocsToExplore; i++) {
            evaluateJoin(blockValueFetcher.getRow(i), rightTable, joinedRows);
          }
        }
        if (joinedRows.size() == _numRowsToKeep) {
          break;
        }
      }
    }
    if (_queryContext.getGroupByExpressions() == null || _queryContext.getGroupByExpressions().size() == 0) {
      // No group by present
      List<String> selectionColumns = _queryContext.getSelectExpressions().stream()
          .map(ExpressionContext::getIdentifier).collect(Collectors.toList());
      DataSchema finalDataSchema = SelectionOperatorUtils.getResultTableDataSchema(_dataSchema, selectionColumns);
      int[] reorderIndex = new int[_dataSchema.size()];
      for (int i = 0; i < _dataSchema.size(); i++) {
        String columnName = _dataSchema.getColumnName(i);
        int idx = ArrayUtils.indexOf(finalDataSchema.getColumnNames(), columnName);
        if (idx != -1) {
          reorderIndex[i] = idx;
        }
      }
      for (int idx = 0; idx < joinedRows.size(); idx++) {
        Object[] row = joinedRows.get(idx);
        Object[] newRow = new Object[finalDataSchema.size()];
        for (int columnNum = 0; columnNum < _dataSchema.size(); columnNum++) {
          if (reorderIndex[columnNum] != -1) {
            newRow[reorderIndex[columnNum]] = row[columnNum];
          }
        }
        joinedRows.set(idx, newRow);
      }
      return new IntermediateResultsBlock(finalDataSchema, joinedRows, _nullHandlingEnabled);
    }
    List<ExpressionContext> groupByExpressions = _queryContext.getGroupByExpressions();
    Preconditions.checkState(groupByExpressions.size() == 1);
    int keyIndex = -1;
    for (int i = 0; i < _dataSchema.size(); i++) {
      if (_dataSchema.getColumnName(i).equals(groupByExpressions.get(0).getIdentifier())) {
        keyIndex = i;
        break;
      }
    }
    AggregationFunction aggregationFunction = _queryContext.getAggregationFunctions()[0];
    Map<Object, Long> groupByResult = new HashMap<>();
    for (Object[] row : joinedRows) {
      groupByResult.computeIfAbsent(row[keyIndex], x -> 0L);
      groupByResult.put(row[keyIndex], (Long) aggregationFunction.merge(1L, groupByResult.get(row[keyIndex])));
    }
    int numGroupByExpressions = _queryContext.getGroupByExpressions().size();
    int numAggFunctions = _queryContext.getAggregationFunctions().length;
    int numColumns = numGroupByExpressions + numAggFunctions;
    String[] columnNames = new String[numColumns];
    DataSchema.ColumnDataType[] columnDataTypes = new DataSchema.ColumnDataType[numColumns];

    // Extract column names and data types for group-by columns
    for (int i = 0; i < numGroupByExpressions; i++) {
      ExpressionContext groupByExpression = _queryContext.getGroupByExpressions().get(i);
      columnNames[i] = groupByExpression.toString();
      for (int j = 0; j < _dataSchema.size(); j++) {
        if (_dataSchema.getColumnName(j).equals(columnNames[i])) {
          columnDataTypes[i] = _dataSchema.getColumnDataType(j);
          break;
        }
      }
    }
    for (int i = 0; i < numAggFunctions; i++) {
      AggregationFunction aggregationFunction1 = _queryContext.getAggregationFunctions()[i];
      int index = numGroupByExpressions + i;
      columnNames[index] = aggregationFunction1.getResultColumnName();
      columnDataTypes[index] = aggregationFunction1.getIntermediateResultColumnType();
    }
    DataSchema finalDataSchema = new DataSchema(columnNames, columnDataTypes);
    List<Object[]> newJoinedRows = new ArrayList<>(groupByResult.size());
    for (Map.Entry<Object, Long> entry : groupByResult.entrySet()) {
      Object[] newRow = new Object[numColumns];
      newRow[0] = entry.getKey();
      newRow[1] = entry.getValue();
      newJoinedRows.add(newRow);
    }
    return new IntermediateResultsBlock(finalDataSchema, newJoinedRows, _nullHandlingEnabled);
  }

  private void computeDataSchema() {
    _finalNumColumns = _leftExpressions.size() + _rightExpressions.size();
    String[] columnNames = new String[_finalNumColumns];
    DataSchema.ColumnDataType[] columnDataTypes = new DataSchema.ColumnDataType[_finalNumColumns];
    int iter = 0;
    for (ExpressionContext expressionContext : _leftExpressions) {
      columnNames[iter] = expressionContext.getIdentifier();
      FieldSpec.DataType dataType = _leftOperators.get(0).getResultMetadata(expressionContext).getDataType();
      columnDataTypes[iter++] = DataSchema.ColumnDataType.fromDataType(dataType, true);
    }
    for (ExpressionContext expressionContext : _rightExpressions) {
      if (Arrays.stream(columnNames).anyMatch(x -> x != null && x.equals(expressionContext.getIdentifier()))) {
        columnNames[iter] = expressionContext.getIdentifier() + "0";
      } else {
        columnNames[iter] = expressionContext.getIdentifier();
      }
      FieldSpec.DataType dataType = _rightOperators.get(0).getResultMetadata(expressionContext).getDataType();
      columnDataTypes[iter++] = DataSchema.ColumnDataType.fromDataType(dataType, true);
    }
    _dataSchema = new DataSchema(columnNames, columnDataTypes);
  }

  private void evaluateJoin(Object[] leftRow, Map<Object, List<Object[]>> hashTable,
      List<Object[]> finalResult) {
    if (hashTable.containsKey(leftRow[_leftJoinIndex])) {
      for (Object[] rightRow : hashTable.get(leftRow[_leftJoinIndex])) {
        finalResult.add(mergeRows(leftRow, rightRow));
      }
    }
  }

  private Object[] mergeRows(Object[] leftRow, Object[] rightRow) {
    Object[] result = new Object[leftRow.length + rightRow.length];
    System.arraycopy(leftRow, 0, result, 0, leftRow.length);
    System.arraycopy(rightRow, 0, result, leftRow.length, rightRow.length);
    return result;
  }

  @Override
  protected void mergeResultsBlocks(IntermediateResultsBlock mergedBlock, IntermediateResultsBlock blockToMerge) {
    throw new UnsupportedOperationException("mergeResultsBlock is not implemented for LocalJoin");
  }

  @Override
  public ExecutionStatistics getExecutionStatistics() {
    return new ExecutionStatistics(0, 0, 0, 0);
  }
}
