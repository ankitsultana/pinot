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
package org.apache.pinot.query.planner.plannode;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.calcite.rel.RelCollation;
import org.apache.pinot.calcite.rel.PinotPhysicalExchangeType;
import org.apache.pinot.calcite.rel.logical.PinotRelExchangeType;
import org.apache.pinot.common.utils.DataSchema;


public class NewExchangeNode extends BasePlanNode {
  private final PinotRelExchangeType _exchangeType;
  private final PinotPhysicalExchangeType _pinotPhysicalExchangeType;
  private final List<Integer> _keys;
  private final RelCollation _collation;
  private final boolean _sortOnSender;
  private final boolean _sortOnReceiver;
  // TODO: Table names should be set for SUB_PLAN exchange type.
  private final Set<String> _tableNames;

  public NewExchangeNode(int stageId, DataSchema dataSchema, List<PlanNode> inputs, PinotRelExchangeType exchangeType,
      @Nullable List<Integer> keys,
      @Nullable RelCollation collation, boolean sortOnSender, boolean sortOnReceiver,
      @Nullable Set<String> tableNames, @Nullable PinotPhysicalExchangeType desc) {
    super(stageId, dataSchema, null, inputs);
    _exchangeType = exchangeType;
    _keys = keys;
    _collation = collation;
    _sortOnSender = sortOnSender;
    _sortOnReceiver = sortOnReceiver;
    _tableNames = tableNames;
    _pinotPhysicalExchangeType = desc;
  }

  public PinotPhysicalExchangeType getPinotExchangeDesc() {
    return _pinotPhysicalExchangeType;
  }

  public PinotRelExchangeType getExchangeType() {
    return _exchangeType;
  }

  @Nullable
  public List<Integer> getKeys() {
    return _keys;
  }

  @Nullable
  public RelCollation getCollation() {
    return _collation;
  }

  public boolean isSortOnSender() {
    return _sortOnSender;
  }

  public boolean isSortOnReceiver() {
    return _sortOnReceiver;
  }

  @Nullable
  public Set<String> getTableNames() {
    return _tableNames;
  }

  @Override
  public String explain() {
    return "NEW_EXCHANGE";
  }

  @Override
  public <T, C> T visit(PlanNodeVisitor<T, C> visitor, C context) {
    return visitor.visitNewExchange(this, context);
  }

  @Override
  public PlanNode withInputs(List<PlanNode> inputs) {
    return new NewExchangeNode(_stageId, _dataSchema, inputs, _exchangeType, _keys,
        _collation, _sortOnSender, _sortOnReceiver, _tableNames, _pinotPhysicalExchangeType);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof NewExchangeNode)) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    NewExchangeNode that = (NewExchangeNode) o;
    return _sortOnSender == that._sortOnSender && _sortOnReceiver == that._sortOnReceiver
        && _exchangeType == that._exchangeType
        && Objects.equals(_keys, that._keys) && Objects.equals(_collation, that._collation)
        && Objects.equals(_tableNames, that._tableNames);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), _exchangeType, _keys, _sortOnSender, _sortOnReceiver,
        _collation, _tableNames);
  }
}
