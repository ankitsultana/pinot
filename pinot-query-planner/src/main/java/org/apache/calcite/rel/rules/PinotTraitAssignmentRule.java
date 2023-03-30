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

import org.apache.calcite.pinot.PinotPlannerSessionContext;
import org.apache.calcite.pinot.traits.PinotRelDistributionTraitDef;
import org.apache.calcite.pinot.PinotRelDistributionTransformer;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.RelBuilderFactory;


public class PinotTraitAssignmentRule extends RelOptRule {
  public static final PinotTraitAssignmentRule INSTANCE =
      new PinotTraitAssignmentRule(PinotRuleUtils.PINOT_REL_FACTORY);

  public PinotTraitAssignmentRule(RelBuilderFactory factory) {
    super(operand(RelNode.class, any()), factory, null);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    if (call.rels.length < 1) {
      return false;
    }
    RelNode relNode = call.rel(0);
    // Only match a node if it doesn't already have a PinotRelDistribution trait
    return relNode.getTraitSet().getTrait(PinotRelDistributionTraitDef.INSTANCE) == null;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    RelNode relNode = call.rel(0);
    PinotPlannerSessionContext context = (PinotPlannerSessionContext) call.getPlanner().getContext();
    call.transformTo(PinotRelDistributionTransformer.dispatch(relNode, context));
  }
}
