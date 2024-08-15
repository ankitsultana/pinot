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

import java.util.List;
import org.apache.pinot.tsdb.spi.series.SeriesBlock;


public abstract class BaseTimeSeriesOperator {
  protected final List<BaseTimeSeriesOperator> _childOperators;

  public BaseTimeSeriesOperator(List<BaseTimeSeriesOperator> childOperators) {
    _childOperators = childOperators;
  }

  public SeriesBlock nextBlock() {
    long startTime = System.currentTimeMillis();
    try {
      return getNextBlock();
    } finally {
      // add stats
    }
  }

  public abstract SeriesBlock getNextBlock();

  public abstract String getExplainName();

  public List<BaseTimeSeriesOperator> getChildOperators() {
    return _childOperators;
  }
}
