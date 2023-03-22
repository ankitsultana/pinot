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
package org.apache.calcite.pinot;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.SortExchange;
import org.apache.calcite.rel.logical.LogicalExchange;


public class PinotExchangeShuttle extends RelShuttleImpl {

  @Override public RelNode visit(LogicalExchange exchange) {
    exchange = (LogicalExchange) super.visitChildren(exchange);
    return PinotExchange.create(exchange);
  }

  @Override
  public RelNode visit(RelNode other) {
    other = super.visitChildren(other);
    if (other instanceof SortExchange) {
      return PinotSortExchange.create((SortExchange) other);
    }
    return other;
  }
}
