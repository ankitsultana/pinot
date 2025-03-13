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

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Sort;
import org.apache.pinot.calcite.rel.PinotDataDistribution;
import org.apache.pinot.calcite.rel.logical.PinotPhysicalExchange;


public class PhysicalPushDownOptimizer {
  private final PlanIdGenerator _idGenerator;

  public PhysicalPushDownOptimizer(PlanIdGenerator idGenerator) {
    _idGenerator = idGenerator;
  }

  public PRelNode pushDown(PRelNode rootNode) {
    // TODO: Need to set isLeafStage.
    List<PRelNode> newInputs = new ArrayList<>();
    for (PRelNode input : rootNode.getInputs()) {
      newInputs.add(pushDown(input));
    }
    boolean isInputExchange = !rootNode.getInputs().isEmpty()
        && rootNode.getInputs().get(0).getRelNode() instanceof PinotPhysicalExchange;
    if (rootNode.getRelNode() instanceof Aggregate && isInputExchange) {
      // TODO: Implement this. Slightly complicated because AggCallList will also change.
    } else if (rootNode.getRelNode() instanceof Sort && isInputExchange) {
      PRelNode oldExchange = newInputs.get(0);
      PRelNode inputPRelNode = newInputs.get(0).getInput(0);
      PinotDataDistribution inputDistribution = inputPRelNode.getPinotDataDistributionOrThrow();
      Map<Integer, List<Integer>> mp = MappingGen.compute(inputPRelNode.getRelNode(), rootNode.getRelNode(), null);
      PinotDataDistribution partialSortDistribution = inputDistribution.apply(mp);
      PRelNode wrappedPartialSort = new PRelNode(_idGenerator.get(), rootNode.getRelNode(),
          partialSortDistribution, ImmutableList.of(inputPRelNode));
      PRelNode newExchange = oldExchange.withNewInputs(oldExchange.getNodeId(), ImmutableList.of(wrappedPartialSort),
          oldExchange.getPinotDataDistributionOrThrow());
      return rootNode.withNewInputs(rootNode.getNodeId(), ImmutableList.of(newExchange),
          rootNode.getPinotDataDistributionOrThrow());
    }
    return rootNode.withNewInputs(rootNode.getNodeId(), newInputs, rootNode.getPinotDataDistributionOrThrow());
  }
}
