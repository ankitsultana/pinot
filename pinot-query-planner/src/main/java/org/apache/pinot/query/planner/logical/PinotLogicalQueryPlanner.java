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
package org.apache.pinot.query.planner.logical;

import java.util.List;
import javax.annotation.Nullable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.core.routing.RoutingManager;
import org.apache.pinot.query.context.PlannerContext;
import org.apache.pinot.query.planner.PlanFragment;
import org.apache.pinot.query.planner.SubPlan;
import org.apache.pinot.query.planner.SubPlanMetadata;
import org.apache.pinot.query.planner.logical.rel2plan.NewRelToPRelConverter;
import org.apache.pinot.query.planner.logical.rel2plan.PRelNode;
import org.apache.pinot.query.planner.physical.Blah;
import org.apache.pinot.query.planner.plannode.PlanNode;


/**
 * PinotLogicalQueryPlanner walks top-down from {@link RelRoot} and construct a forest of trees with {@link PlanNode}.
 */
public class PinotLogicalQueryPlanner {
  private PinotLogicalQueryPlanner() {
  }

  /**
   * Converts a Calcite {@link RelRoot} into a Pinot {@link SubPlan}.
   */
  public static Pair<SubPlan, Blah.Result> makePlan(RelRoot relRoot,
      @Nullable TransformationTracker.Builder<PlanNode, RelNode> tracker, boolean useSpools,
      RoutingManager routingManager, long requestId, PlannerContext plannerContext) {
    // Step-1: Convert RelNode tree to a PRelNode
    NewRelToPRelConverter relToPrelConverter = new NewRelToPRelConverter(requestId, plannerContext);
    PRelNode pRelNode = relToPrelConverter.toPRelNode(relRoot.rel);
    Blah blah = new Blah();
    Blah.Result blahResult = blah.compute(pRelNode, plannerContext.getPhysicalPlannerContext());
    PlanFragment rootFragment = blahResult._planFragmentMap.get(0);
    // Step-3: Create plan fragments.
    SubPlan subPlan = new SubPlan(rootFragment,
        new SubPlanMetadata(RelToPlanNodeConverter.getTableNamesFromRelRoot(relRoot.rel), relRoot.fields), List.of());
    return Pair.of(subPlan, blahResult);
  }
}
