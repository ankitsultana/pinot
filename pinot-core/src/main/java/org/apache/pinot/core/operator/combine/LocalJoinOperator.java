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

import java.util.Collection;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.core.common.Block;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;


public class LocalJoinOperator extends BaseOperator<IntermediateResultsBlock> {
  private final List<Operator<IntermediateResultsBlock>> _leftOperators;
  private final BaseCombineOperator _rightOperator;

  public LocalJoinOperator(List<Operator<IntermediateResultsBlock>> leftOperators,
      BaseCombineOperator rightCombineOperator) {
    _leftOperators = leftOperators;
    _rightOperator = rightCombineOperator;
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
    IntermediateResultsBlock rightBlock = _rightOperator.getNextBlock();
    Collection<Object[]> result = rightBlock.getSelectionResult();
    for (Operator operator : _leftOperators) {
      Block block = operator.nextBlock();
    }
    return null;
  }
}
