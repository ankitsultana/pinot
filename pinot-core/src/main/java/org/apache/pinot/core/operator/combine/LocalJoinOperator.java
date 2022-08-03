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

  public LocalJoinOperator(List<Operator<IntermediateResultsBlock>> leftOperators, BaseCombineOperator rightCombineOperator) {
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
