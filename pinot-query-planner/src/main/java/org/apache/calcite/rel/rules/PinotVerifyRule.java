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

import com.google.common.base.Preconditions;
import org.apache.calcite.pinot.traits.PinotRelDistributionTraitDef;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.commons.collections.CollectionUtils;


public class PinotVerifyRule extends RelOptRule {
  public static final PinotVerifyRule INSTANCE = new PinotVerifyRule(PinotRuleUtils.PINOT_REL_FACTORY);

  protected PinotVerifyRule(RelBuilderFactory factory) {
    super(operand(RelNode.class, any()), factory, null);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    if (call.rels.length < 1) {
      return false;
    }
    Preconditions.checkState(call.rels.length == 1);
    return true;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    RelNode relNode = call.rel(0);
    Preconditions.checkState(
        CollectionUtils.isNotEmpty(relNode.getTraitSet().getTraits(PinotRelDistributionTraitDef.INSTANCE)),
        String.format("PinotRelDistribution not set for RelNode: %s", relNode.getClass()));
  }
}