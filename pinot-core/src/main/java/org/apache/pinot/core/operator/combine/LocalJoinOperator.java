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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.common.RowBasedBlockValueFetcher;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;
import org.apache.pinot.core.operator.blocks.TransformBlock;
import org.apache.pinot.core.operator.transform.TransformOperator;
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
  private final int _finalNumColumns;
  private final DataSchema _dataSchema;

  private final int[] _lMap;
  private final int[] _rMap;

  private int _numDocsScanned = 0;

  public LocalJoinOperator(List<TransformOperator> leftOperators,
      List<TransformOperator> rightOperators, Collection<ExpressionContext> leftExpressions,
      Collection<ExpressionContext> rightExpressions, QueryContext queryContext, ExecutorService executorService) {
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

    _finalNumColumns = queryContext.getColumns().size();
    String[] columnNames = new String[_finalNumColumns];
    DataSchema.ColumnDataType[] columnDataTypes = new DataSchema.ColumnDataType[_finalNumColumns];
    int iter = 0;
    for (ExpressionContext expressionContext : queryContext.getSelectExpressions()) {
      columnNames[iter++] = expressionContext.getIdentifier();
    }

    _lMap = new int[_leftExpressions.size()];
    _rMap = new int[_rightExpressions.size()];
    for (int i = 0; i < _lMap.length; i++) {
      _lMap[i] = -1;
    }
    for (int i = 0; i < _rMap.length; i++) {
      _rMap[i] = -1;
    }

    for (int leftIndex = 0; leftIndex < _leftExpressions.size(); leftIndex++) {
      for (int i = 0; i < _finalNumColumns; i++) {
        if (_leftExpressions.get(leftIndex).getIdentifier().equals(columnNames[i])) {
          _lMap[leftIndex] = i;
          break;
        }
      }
    }

    Set<String> leftLookUpSet = _leftExpressions.stream().map(ExpressionContext::getIdentifier)
        .collect(Collectors.toSet());
    for (int rightIndex = 0; rightIndex < _rightExpressions.size(); rightIndex++) {
      String rightIdentifier = _rightExpressions.get(rightIndex).getIdentifier();
      if (leftLookUpSet.contains(rightIdentifier)) {
        rightIdentifier = rightIdentifier + "0";
      }
      for (int i = 0; i < _finalNumColumns; i++) {
        if (rightIdentifier.equals(columnNames[i])) {
          _rMap[rightIndex] = i;
          break;
        }
      }
    }

    for (int i = 0; i < _leftExpressions.size(); i++) {
      FieldSpec.DataType dataType = _leftOperators.get(0).getResultMetadata(_leftExpressions.get(i)).getDataType();
      if (_lMap[i] != -1) {
        columnDataTypes[_lMap[i]] = DataSchema.ColumnDataType.fromDataType(dataType, true);
      }
    }
    for (int i = 0; i < _rightExpressions.size(); i++) {
      FieldSpec.DataType dataType = _rightOperators.get(0).getResultMetadata(_rightExpressions.get(i)).getDataType();
      if (_rMap[i] != -1) {
        columnDataTypes[_rMap[i]] = DataSchema.ColumnDataType.fromDataType(dataType, true);
      }
    }

    _dataSchema = new DataSchema(columnNames, columnDataTypes);
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
        _numDocsScanned += numDocsToAdd;
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
        _numDocsScanned += numDocsToExplore;
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
    return new IntermediateResultsBlock(_dataSchema, joinedRows, _nullHandlingEnabled);
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
    for (int i = 0; i < leftRow.length; i++) {
      if (_lMap[i] != -1) {
        result[_lMap[i]] = leftRow[i];
      }
    }
    for (int i = 0; i < rightRow.length; i++) {
      if (_rMap[i] != -1) {
        result[_rMap[i]] = rightRow[i];
      }
    }
    return result;
  }

  @Override
  protected void mergeResultsBlocks(IntermediateResultsBlock mergedBlock, IntermediateResultsBlock blockToMerge) {
    throw new UnsupportedOperationException("mergeResultsBlock is not implemented for LocalJoin");
  }
}
