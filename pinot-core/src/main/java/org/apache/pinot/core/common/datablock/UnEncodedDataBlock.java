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
package org.apache.pinot.core.common.datablock;

import java.util.Collection;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataTable;


public class UnEncodedDataBlock extends BaseDataBlock {
  private DataSchema _dataSchema;
  private Collection<Object[]> _rows;

  public UnEncodedDataBlock() {
    super();
  }

  public UnEncodedDataBlock(Collection<Object[]> rows, DataSchema dataSchema) {
    super();
    _rows = rows;
    _dataSchema = dataSchema;
    _numRows = _rows.size();
  }

  @Override
  protected int getDataBlockVersionType() {
    return 0;
  }

  @Override
  protected int getOffsetInFixedBuffer(int rowId, int colId) {
    return 0;
  }

  @Override
  protected int positionOffsetInVariableBufferAndGetLength(int rowId, int colId) {
    return 0;
  }

  @Override
  public DataTable toMetadataOnlyDataTable() {
    return null;
  }

  @Override
  public DataTable toDataOnlyDataTable() {
    return null;
  }

  public Collection<Object[]> getRows() {
    return _rows;
  }
}
