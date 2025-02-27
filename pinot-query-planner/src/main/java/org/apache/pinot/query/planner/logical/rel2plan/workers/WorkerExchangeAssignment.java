package org.apache.pinot.query.planner.logical.rel2plan.workers;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
import org.apache.pinot.calcite.rel.PinotDataDistribution;
import org.apache.pinot.calcite.rel.PinotExchangeDesc;
import org.apache.pinot.calcite.rel.logical.PinotPhysicalExchange;
import org.apache.pinot.query.planner.logical.rel2plan.MappingGen;
import org.apache.pinot.query.planner.logical.rel2plan.PlanIdGenerator;
import org.apache.pinot.query.planner.logical.rel2plan.WrappedRelNode;


/**
 * 1. Partial aggregate.
 * 2. Logical sort.
 */
public class WorkerExchangeAssignment extends BaseWorkerExchangeAssignment {
  private final PlanIdGenerator _idGenerator;

  public WorkerExchangeAssignment(PlanIdGenerator idGenerator) {
    _idGenerator = idGenerator;
  }

  @Override
  public WrappedRelNode assign(WrappedRelNode rootNode) {
    return assignRecursive(rootNode, null);
  }

  private WrappedRelNode assignRecursive(WrappedRelNode currentNode, @Nullable PinotDataDistribution parentDistribution) {
    WrappedRelNode result = assignRecursiveInternal(currentNode, parentDistribution);
    return result;
  }

  // TODO: Handle parallelism.
  // TODO: Support old features like "pre-partitioned".
  private WrappedRelNode assignRecursiveInternal(WrappedRelNode currentNode, @Nullable PinotDataDistribution parentDistribution) {
    // Step-1: Assign for left input first
    List<WrappedRelNode> newInputs = new ArrayList<>();
    if (!currentNode.getInputs().isEmpty()) {
      newInputs.add(assignRecursive(currentNode.getInputs().get(0), null));
    }
    if (currentNode.isLeafStage() && !currentNode.isLeafStageBoundary()) {
      return currentNode.copy(currentNode.getNodeId(), newInputs, currentNode.getPinotDataDistribution().get());
    }
    // Step-2: Assign to current node
    boolean isStageBoundary = currentNode.isLeafStageBoundary();
    PinotDataDistribution inputDataDistribution = currentNode.getInputs().get(0).getPinotDataDistribution().get();
    PinotDataDistribution currentNodeDistribution =
        inputDataDistribution.apply(MappingGen.compute(currentNode.getRelNode(), newInputs.get(0).getRelNode(), null));
    RelDistribution relDistribution = coalesceDistribution(currentNode.getRelNode().getTraitSet().getDistribution());
    RelCollation relCollation = coalesceCollation(currentNode.getRelNode().getTraitSet().getCollation());
    boolean isDistributionSatisfied = currentNodeDistribution.satisfies(relDistribution);
    boolean isCollationSatisfied = currentNodeDistribution.satisfies(relCollation);
    WrappedRelNode currentNodeExchange = null;
    if (isDistributionSatisfied && isCollationSatisfied) {
      if (isStageBoundary) {
        if (parentDistribution == null) {
          PinotPhysicalExchange physicalExchange = new PinotPhysicalExchange(currentNode.getRelNode(),
              Collections.emptyList(), PinotExchangeDesc.IDENTITY_EXCHANGE);
          currentNodeExchange = new WrappedRelNode(_idGenerator.get(), physicalExchange, currentNodeDistribution);
        } else {
          currentNodeExchange = meetDistributionConstraint(currentNode, relDistribution, parentDistribution, currentNodeDistribution);
        }
      } else if (parentDistribution != null) {
        currentNodeExchange = meetDistributionConstraint(currentNode, relDistribution, parentDistribution, currentNodeDistribution);
      }
    } else if (!isDistributionSatisfied) {
      Preconditions.checkState(relCollation == RelCollations.EMPTY);
      if (parentDistribution == null) {
        currentNodeExchange = meetDistributionConstraint(currentNode, relDistribution);
      } else {
        currentNodeExchange = meetDistributionConstraint(currentNode, relDistribution, parentDistribution, currentNodeDistribution);
      }
    } else if (!isCollationSatisfied) {
      if (parentDistribution == null) {
        currentNodeExchange = meetCollationConstraint(currentNode, relCollation, currentNodeDistribution);
      } else {
        throw new IllegalStateException("Don't support collation under a join yet");
      }
    } else {
      // TODO: Do both of the above.
      throw new IllegalStateException("Can't do both collation and distribution yet");
    }
    // Step-3: Assign to other inputs.
    for (int inputIndex = 1; inputIndex < currentNode.getInputs().size(); inputIndex++) {
      newInputs.add(assignRecursive(currentNode.getInputs().get(inputIndex), currentNodeDistribution));
    }
    // Step-4: Return the correct node above.
    if (currentNodeExchange != null) {
      WrappedRelNode currentNodeWithNewInputs = currentNode.copy(currentNode.getNodeId(), newInputs, currentNodeDistribution);
      return currentNodeExchange.copy(currentNodeExchange.getNodeId(), ImmutableList.of(currentNodeWithNewInputs), null);
    }
    return currentNode.copy(currentNode.getNodeId(), newInputs, currentNodeDistribution);
  }

  private WrappedRelNode meetCollationConstraint(WrappedRelNode currentNode, RelCollation collation, PinotDataDistribution assumedDistribution) {
    List<Integer> collationKeys = new ArrayList<>(collation.getKeys());
    if (assumedDistribution.getType() == PinotDataDistribution.Type.HASH_PARTITIONED) {
      PinotDataDistribution.HashDistributionDesc desc = assumedDistribution.getHashDistributionDesc().iterator().next();
      if (desc.getKeyIndexes().equals(collationKeys)) {
        PinotPhysicalExchange physicalExchange = new PinotPhysicalExchange(currentNode.getRelNode(), collationKeys, PinotExchangeDesc.IDENTITY_EXCHANGE, true, collation.getFieldCollations().get(0).getDirection());
        return new WrappedRelNode(_idGenerator.get(), physicalExchange, assumedDistribution);
      }
    }
    PinotDataDistribution.HashDistributionDesc desc = new PinotDataDistribution.HashDistributionDesc(collationKeys,
        "murmur", assumedDistribution.getWorkers().size(), 1);
    PinotDataDistribution pinotDataDistribution = new PinotDataDistribution(PinotDataDistribution.Type.HASH_PARTITIONED,
        assumedDistribution.getWorkers(), assumedDistribution.getWorkerHash(), ImmutableSet.of(desc),
        collationKeys, collation.getFieldCollations());
    PinotPhysicalExchange physicalExchange = new PinotPhysicalExchange(currentNode.getRelNode(), collationKeys, PinotExchangeDesc.PARTITIONING_EXCHANGE,
        true, collation.getFieldCollations().get(0).getDirection());
    return new WrappedRelNode(_idGenerator.get(), physicalExchange, pinotDataDistribution);
  }

  /**
   * There's no parent distribution and given distribution is not satisfied with default assignment.
   */
  private WrappedRelNode meetDistributionConstraint(WrappedRelNode currentNode, RelDistribution distributionConstraint) {
    PinotDataDistribution inputDataDistribution = currentNode.getInputs().get(0).getPinotDataDistribution().get();
    if (distributionConstraint.getType() == RelDistribution.Type.BROADCAST_DISTRIBUTED) {
      PinotPhysicalExchange physicalExchange = new PinotPhysicalExchange(currentNode.getRelNode(),
          Collections.emptyList(), PinotExchangeDesc.BROADCAST_EXCHANGE);
      PinotDataDistribution pinotDataDistribution = new PinotDataDistribution(PinotDataDistribution.Type.BROADCAST,
          inputDataDistribution.getWorkers(), inputDataDistribution.getWorkerHash(), null, null, null);
      return new WrappedRelNode(_idGenerator.get(), physicalExchange, pinotDataDistribution);
    }
    if (distributionConstraint.getType() == RelDistribution.Type.SINGLETON) {
      PinotPhysicalExchange physicalExchange = new PinotPhysicalExchange(currentNode.getRelNode(),
          Collections.emptyList(), PinotExchangeDesc.SINGLETON_EXCHANGE);
      List<String> newWorkers = inputDataDistribution.getWorkers().subList(0, 1);
      PinotDataDistribution pinotDataDistribution = new PinotDataDistribution(PinotDataDistribution.Type.SINGLETON,
          newWorkers, newWorkers.hashCode(), null, null, null);
      return new WrappedRelNode(_idGenerator.get(), physicalExchange, pinotDataDistribution);
    }
    if (distributionConstraint.getType() == RelDistribution.Type.HASH_DISTRIBUTED) {
      PinotPhysicalExchange physicalExchange = new PinotPhysicalExchange(currentNode.getRelNode(),
          Collections.emptyList(), PinotExchangeDesc.PARTITIONING_EXCHANGE);
      PinotDataDistribution.HashDistributionDesc desc = new PinotDataDistribution.HashDistributionDesc(
          distributionConstraint.getKeys(), "murmur", inputDataDistribution.getWorkers().size(), 1);
      PinotDataDistribution pinotDataDistribution = new PinotDataDistribution(PinotDataDistribution.Type.HASH_PARTITIONED,
          inputDataDistribution.getWorkers(), inputDataDistribution.getWorkerHash(), ImmutableSet.of(desc), null, null);
      return new WrappedRelNode(_idGenerator.get(), physicalExchange, pinotDataDistribution);
    }
    throw new IllegalStateException("Distribution constraint not met: " + distributionConstraint.getType());
  }

  @Nullable
  private WrappedRelNode meetDistributionConstraint(WrappedRelNode currentNode, RelDistribution relDistribution,
      PinotDataDistribution parentDistribution, PinotDataDistribution assumedDistribution) {
    if (!assumedDistribution.satisfies(relDistribution)) {
      // if assumed distribution does not satisfy constraint any ways, then a full exchange is required.
      if (relDistribution.getType() == RelDistribution.Type.BROADCAST_DISTRIBUTED) {
        PinotDataDistribution pinotDataDistribution = new PinotDataDistribution(PinotDataDistribution.Type.BROADCAST,
            parentDistribution.getWorkers(), parentDistribution.getWorkerHash(), null, null, null);
        PinotPhysicalExchange physicalExchange = new PinotPhysicalExchange(currentNode.getRelNode(),
            Collections.emptyList(), PinotExchangeDesc.BROADCAST_EXCHANGE);
        return new WrappedRelNode(_idGenerator.get(), physicalExchange, pinotDataDistribution);
      }
      if (relDistribution.getType() == RelDistribution.Type.SINGLETON) {
        Preconditions.checkState(parentDistribution.getWorkers().size() == 1, "Singleton constraint but parent node has more than 1 worker");
        PinotDataDistribution pinotDataDistribution = new PinotDataDistribution(PinotDataDistribution.Type.SINGLETON,
            parentDistribution.getWorkers(), parentDistribution.getWorkerHash(), null, null, null);
        PinotPhysicalExchange physicalExchange = new PinotPhysicalExchange(currentNode.getRelNode(),
            Collections.emptyList(), PinotExchangeDesc.SINGLETON_EXCHANGE);
        return new WrappedRelNode(_idGenerator.get(), physicalExchange, pinotDataDistribution);
      }
      if (relDistribution.getType() == RelDistribution.Type.HASH_DISTRIBUTED) {
        // get num-partitions from parentDistribution
        int numPartitions = parentDistribution.getWorkers().size();
        if (parentDistribution.getType() == PinotDataDistribution.Type.HASH_PARTITIONED) {
          numPartitions = parentDistribution.getHashDistributionDesc().iterator().next().getNumPartitions();
        }
        PinotDataDistribution.HashDistributionDesc desc = new PinotDataDistribution.HashDistributionDesc(
            relDistribution.getKeys(), "murmur", numPartitions, 1);
        PinotDataDistribution pinotDataDistribution = new PinotDataDistribution(PinotDataDistribution.Type.HASH_PARTITIONED,
            parentDistribution.getWorkers(), parentDistribution.getWorkerHash(), ImmutableSet.of(desc), null, null);
        PinotPhysicalExchange physicalExchange = new PinotPhysicalExchange(currentNode.getRelNode(),
            relDistribution.getKeys(), PinotExchangeDesc.PARTITIONING_EXCHANGE);
        return new WrappedRelNode(_idGenerator.get(), physicalExchange, pinotDataDistribution);
      }
      throw new IllegalStateException("Unexpected unsatisfied rel distribution type: " + relDistribution.getType());
    }
    if (assumedDistribution.getWorkerHash() == parentDistribution.getWorkerHash()) {
      return null;
    }
    // workers are different but constraint is satisfied.
    if (relDistribution.getType() == RelDistribution.Type.BROADCAST_DISTRIBUTED) {
      // TODO: Can do this with permutation based exchange
      throw new IllegalStateException("Can't do broadcast to broadcast with different servers");
    }
    if (relDistribution.getType() == RelDistribution.Type.SINGLETON) {
      PinotDataDistribution pinotDataDistribution = new PinotDataDistribution(PinotDataDistribution.Type.SINGLETON,
          parentDistribution.getWorkers(), parentDistribution.getWorkerHash(), null, null, null);
      PinotPhysicalExchange physicalExchange = new PinotPhysicalExchange(currentNode.getRelNode(),
          Collections.emptyList(), PinotExchangeDesc.SINGLETON_EXCHANGE);
      return new WrappedRelNode(_idGenerator.get(), physicalExchange, pinotDataDistribution);
    }
    if (relDistribution.getType() == RelDistribution.Type.ANY) {
      // TODO: random assignment.
    }
    if (relDistribution.getType() == RelDistribution.Type.HASH_DISTRIBUTED) {
      int numDesiredPartitions = parentDistribution.getWorkers().size();
      if (parentDistribution.getType() == PinotDataDistribution.Type.HASH_PARTITIONED) {
        numDesiredPartitions = parentDistribution.getHashDistributionDesc().iterator().next().getNumPartitions();
      }
      int currentNumPartitions = assumedDistribution.getHashDistributionDesc().iterator().next().getNumPartitions();
      if (currentNumPartitions == numDesiredPartitions && assumedDistribution.getWorkers().size() == parentDistribution.getWorkers().size()) {
        // identity exchange
        PinotDataDistribution pinotDataDistribution = new PinotDataDistribution(PinotDataDistribution.Type.HASH_PARTITIONED,
            parentDistribution.getWorkers(), parentDistribution.getWorkerHash(), assumedDistribution.getHashDistributionDesc(),
            null, null);
        PinotPhysicalExchange physicalExchange = new PinotPhysicalExchange(currentNode.getRelNode(),
            Collections.emptyList(), PinotExchangeDesc.IDENTITY_EXCHANGE);
        return new WrappedRelNode(_idGenerator.get(), physicalExchange, pinotDataDistribution);
      }
      if (numDesiredPartitions % currentNumPartitions == 0 && assumedDistribution.getWorkers().size() == parentDistribution.getWorkers().size()) {
        Optional<PinotDataDistribution.HashDistributionDesc>
            descOptional = assumedDistribution.getHashDistributionDesc().stream().filter(x -> x.getKeyIndexes().equals(relDistribution.getKeys())).findFirst();
        Preconditions.checkState(descOptional.isPresent());
        PinotDataDistribution pinotDataDistribution = new PinotDataDistribution(PinotDataDistribution.Type.HASH_PARTITIONED,
            parentDistribution.getWorkers(), parentDistribution.getWorkerHash(), ImmutableSet.of(descOptional.get()),
            null, null);
        PinotPhysicalExchange physicalExchange = new PinotPhysicalExchange(currentNode.getRelNode(),
            relDistribution.getKeys(), PinotExchangeDesc.SUB_PARTITIONING_HASH_EXCHANGE);
        return new WrappedRelNode(_idGenerator.get(), physicalExchange, pinotDataDistribution);
      }
      PinotDataDistribution.HashDistributionDesc desc = new PinotDataDistribution.HashDistributionDesc(
          relDistribution.getKeys(), "murmur", numDesiredPartitions, 1);
      PinotDataDistribution pinotDataDistribution = new PinotDataDistribution(PinotDataDistribution.Type.HASH_PARTITIONED,
          parentDistribution.getWorkers(), parentDistribution.getWorkerHash(), ImmutableSet.of(desc),
          null, null);
      PinotPhysicalExchange physicalExchange = new PinotPhysicalExchange(currentNode.getRelNode(),
          relDistribution.getKeys(), PinotExchangeDesc.PARTITIONING_EXCHANGE);
      return new WrappedRelNode(_idGenerator.get(), physicalExchange, pinotDataDistribution);
    }
    throw new IllegalStateException("");
  }

  private RelDistribution coalesceDistribution(@Nullable RelDistribution distribution) {
    return distribution == null ? RelDistributions.ANY : distribution;
  }

  private RelCollation coalesceCollation(@Nullable RelCollation collation) {
    return collation == null ? RelCollations.EMPTY : collation;
  }
}
