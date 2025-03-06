package org.apache.pinot.query.planner.logical.rel2plan;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.RelNode;
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
      Aggregate aggregate = (Aggregate) rootNode.getRelNode();
      PRelNode oldExchange = newInputs.get(0);
      PRelNode inputPRelNode = newInputs.get(0).getInput(0);
      PinotDataDistribution inputDistribution = inputPRelNode.getPinotDataDistribution().get();
      Map<Integer, Integer> mp = MappingGen.compute(inputPRelNode.getRelNode(), rootNode.getRelNode(), null);
      PinotDataDistribution partialAggregateDistribution = inputDistribution.apply(mp);
      PRelNode wrappedPartialAggregate = new PRelNode(_idGenerator.get(), rootNode.getRelNode(),
          partialAggregateDistribution);
      wrappedPartialAggregate.addInput(inputPRelNode);
      PRelNode newExchange = oldExchange.copy(oldExchange.getNodeId(), ImmutableList.of(wrappedPartialAggregate),
              oldExchange.getPinotDataDistribution().get());
      return rootNode.copy(rootNode.getNodeId(), ImmutableList.of(newExchange),
          rootNode.getPinotDataDistribution().get());
    } else if (rootNode.getRelNode() instanceof Sort && isInputExchange) {
      PRelNode oldExchange = newInputs.get(0);
      PRelNode inputPRelNode = newInputs.get(0).getInput(0);
      PinotDataDistribution inputDistribution = inputPRelNode.getPinotDataDistribution().get();
      Map<Integer, Integer> mp = MappingGen.compute(inputPRelNode.getRelNode(), rootNode.getRelNode(), null);
      PinotDataDistribution partialSortDistribution = inputDistribution.apply(mp);
      PRelNode wrappedPartialSort = new PRelNode(_idGenerator.get(), rootNode.getRelNode(),
          partialSortDistribution);
      wrappedPartialSort.addInput(inputPRelNode);
      PRelNode newExchange = oldExchange.copy(oldExchange.getNodeId(), ImmutableList.of(wrappedPartialSort),
          oldExchange.getPinotDataDistribution().get());
      return rootNode.copy(rootNode.getNodeId(), ImmutableList.of(newExchange),
          rootNode.getPinotDataDistribution().get());
    }
    return rootNode.copy(rootNode.getNodeId(), newInputs, rootNode.getPinotDataDistribution().get());
  }
}
