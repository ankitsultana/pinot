package org.apache.pinot.core.operator.transform.function;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.spi.data.FieldSpec.DataType;


public class TimeSeriesTransformFunction extends BaseTransformFunction {
  public static final String FUNCTION_NAME = "timeSeriesSeconds";
  private TimeUnit _timeUnit;
  private long _reference = -1;
  private long _divisor = -1;
  private long _offsetSeconds = 0;

  @Override
  public void init(List<TransformFunction> arguments, Map<String, ColumnContext> columnContextMap) {
    super.init(arguments, columnContextMap);
    _timeUnit = TimeUnit.valueOf(((LiteralTransformFunction) arguments.get(1)).getStringLiteral());
    _offsetSeconds = ((LiteralTransformFunction) arguments.get(4)).getLongLiteral();
    final long startSeconds = ((LiteralTransformFunction) arguments.get(2)).getLongLiteral();
    final long bucketSizeSeconds = ((LiteralTransformFunction) arguments.get(3)).getLongLiteral();
    _reference = (startSeconds - bucketSizeSeconds);
    _divisor = bucketSizeSeconds;
    if (_timeUnit == TimeUnit.MILLISECONDS) {
      _reference *= 1000;
      _divisor *= 1000;
    }
  }

  @Override
  public int[] transformToIntValuesSV(ValueBlock valueBlock) {
    int length = valueBlock.getNumDocs();
    initIntValuesSV(length);
    long[] inputValues = _arguments.get(0).transformToLongValuesSV(valueBlock);
    for (int docIndex = 0; docIndex < length; docIndex++) {
      _intValuesSV[docIndex] = (int) (((inputValues[docIndex] + _offsetSeconds) - _reference - 1) / _divisor);
    }
    return _intValuesSV;
  }

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return new TransformResultMetadata(DataType.INT, true, false);
  }
}
