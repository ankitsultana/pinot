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
package org.apache.calcite.rel.rules;

import org.apache.calcite.pinot.ExchangeFactory;
import org.apache.calcite.pinot.PinotExchange;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.logical.LogicalWindow;
import org.apache.calcite.tools.RelBuilderFactory;


/**
 * Special rule for Pinot, this rule is fixed to always insert an exchange or sort exchange below the WINDOW node.
 * TODO:
 *     1. Add support for more than one window group
 *     2. Add support for functions other than aggregation functions (AVG, COUNT, MAX, MIN, SUM, BOOL_AND, BOOL_OR)
 *     3. Add support for custom frames
 */
public class PinotWindowExchangeNodeInsertRule extends RelOptRule {
  public static final PinotWindowExchangeNodeInsertRule INSTANCE =
      new PinotWindowExchangeNodeInsertRule(PinotRuleUtils.PINOT_REL_FACTORY);

  public PinotWindowExchangeNodeInsertRule(RelBuilderFactory factory) {
    super(operand(LogicalWindow.class, any()), factory, null);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    if (call.rels.length < 1) {
      return false;
    }
    if (call.rel(0) instanceof Window) {
      Window window = call.rel(0);
      // Only run the rule if the input isn't already an exchange node
      return !PinotRuleUtils.isExchange(window.getInput());
    }
    return false;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    Window window = call.rel(0);

    PinotExchange exchange = ExchangeFactory.create(window);
    Window newWindow = LogicalWindow.create(window.getTraitSet(), exchange, window.getConstants(), window.getRowType(),
        window.groups);
    call.transformTo(newWindow);
  }
}
