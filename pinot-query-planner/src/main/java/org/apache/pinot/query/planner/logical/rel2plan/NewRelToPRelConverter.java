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
package org.apache.pinot.query.planner.logical.rel2plan;

import java.util.Map;
import org.apache.calcite.rel.RelNode;
import org.apache.pinot.query.context.PhysicalPlannerContext;
import org.apache.pinot.query.context.PlannerContext;
import org.apache.pinot.query.planner.logical.rel2plan.workers.BaseWorkerExchangeAssignment;
import org.apache.pinot.query.planner.logical.rel2plan.workers.LiteModeWorkerExchangeAssignment;
import org.apache.pinot.query.planner.logical.rel2plan.workers.WorkerExchangeAssignment;
import org.apache.pinot.query.planner.physical.v2.PRelOptRule;
import org.apache.pinot.query.planner.physical.v2.PhysicalQueryRuleSet;
import org.apache.pinot.query.planner.physical.v2.RuleExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class NewRelToPRelConverter {
  private static final Logger LOGGER = LoggerFactory.getLogger(NewRelToPRelConverter.class);
  private static final int DEFAULT_STAGE_ID = -1;
  private final LeafWorkerAssignment _leafWorkerAssignment;
  private final long _requestId;
  private final boolean _useLiteMode;

  public NewRelToPRelConverter(long requestId, PlannerContext plannerContext) {
    _leafWorkerAssignment = new LeafWorkerAssignment(plannerContext.getPhysicalPlannerContext());
    _requestId = requestId;
    _useLiteMode = plannerContext.getOptions().containsKey("useLiteMode");
  }

  /**
   */
  public PRelNode toPRelNode(RelNode relNode) {
    // TODO: Step-0: Identify sub-plans, divide between broker and server sub-plan.
    // Step-1: Create Physical RelNode tree.
    PlanIdGenerator generator = new PlanIdGenerator();
    PRelNode pRelNode = PRelNode.wrapRelTree(relNode, generator);
    // Step-2: Compute primitive leaf stage boundary (only single project / filter / scan allowed).
    LeafStageBoundaryComputer leafStageBoundaryComputer = new LeafStageBoundaryComputer(generator);
    pRelNode = leafStageBoundaryComputer.compute(pRelNode);
    // Step-3: Assign workers to leaf stage nodes.
    pRelNode = _leafWorkerAssignment.compute(pRelNode, _requestId);
    // Step-4: Assign workers to all nodes.
    BaseWorkerExchangeAssignment workerExchangeAssignment;
    if (!_useLiteMode) {
      workerExchangeAssignment = new WorkerExchangeAssignment(generator);
    } else {
      workerExchangeAssignment = new LiteModeWorkerExchangeAssignment(generator);
    }
    pRelNode = workerExchangeAssignment.assign(pRelNode);
    // Step-5: Push down sort and aggregate.
    pRelNode = new PhysicalPushDownOptimizer(generator).pushDown(pRelNode);
    // Step-6: Replace logical aggregate with pinot logical aggregate.
    LogicalAggregateConverter logicalAggregateConverter = new LogicalAggregateConverter();
    pRelNode = logicalAggregateConverter.convert(pRelNode);
    PRelNode.printWrappedRelNode(pRelNode, 0);
    return pRelNode;
  }

  public PRelNode toPRelNodeV2(RelNode relNode, PhysicalPlannerContext context, Map<String, String> queryOptions) {
    PlanIdGenerator generator = new PlanIdGenerator();
    PRelNode pRelNode = PRelNode.wrapRelTree(relNode, generator);
    var rules = PhysicalQueryRuleSet.create(context, queryOptions);
    for (var ruleAndExecutor : rules) {
      PRelOptRule rule = ruleAndExecutor.getLeft();
      RuleExecutor executor = ruleAndExecutor.getRight();
      pRelNode = executor.execute(pRelNode, rule, context);
    }
    return pRelNode;
  }
}
