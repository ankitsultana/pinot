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
package org.apache.pinot.tsdb.spi.operator;

import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.tsdb.spi.TimeBuckets;
import org.apache.pinot.tsdb.spi.series.SeriesBlock;


public abstract class BasePipelineOperator extends BaseTimeSeriesOperator {
  protected final List<? extends BaseTimeSeriesOperator> _childOperators;

  protected BasePipelineOperator(List<? extends BaseTimeSeriesOperator> childOperators) {
    super((List<BaseTimeSeriesOperator>) childOperators);
    _childOperators = childOperators;
  }

  public abstract SeriesBlock merge(List<SeriesBlock> allBlocks);

  @Override
  public SeriesBlock getNextBlock() {
    List<SeriesBlock> inputBlocks = new ArrayList<>(_childOperators.size());
    for (BaseTimeSeriesOperator pipelineOperator : _childOperators) {
      inputBlocks.add(pipelineOperator.nextBlock());
    }
    return merge(inputBlocks);
  }

  protected TimeBuckets validateTimeBuckets(List<SeriesBlock> allBlocks) {
    TimeBuckets result = null;
    for (SeriesBlock seriesBlock : allBlocks) {
      if (result == null) {
        result = seriesBlock.getTimeBuckets();
      } else {
        if (!result.equals(seriesBlock.getTimeBuckets())) {
          throw new IllegalStateException("Attempting to merge pipelines with different time buckets");
        }
      }
    }
    return result;
  }
}
