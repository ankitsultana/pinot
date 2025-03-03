package org.apache.pinot.query.planner.logical.rel2plan.workers;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
import org.apache.pinot.calcite.rel.PinotDataDistribution;
import org.apache.pinot.calcite.rel.PinotExchangeDesc;
import org.apache.pinot.calcite.rel.logical.PinotPhysicalExchange;
import org.apache.pinot.query.planner.logical.rel2plan.MappingGen;
import org.apache.pinot.query.planner.logical.rel2plan.PRelNode;
import org.apache.pinot.query.planner.logical.rel2plan.PlanIdGenerator;


/**
 */
public class WorkerExchangeAssignment extends BaseWorkerExchangeAssignment {
  private final PlanIdGenerator _idGenerator;

  public WorkerExchangeAssignment(PlanIdGenerator idGenerator) {
    _idGenerator = idGenerator;
  }

  @Override
  public PRelNode assign(PRelNode rootNode) {
    return assignRecursiveInternal(rootNode, null);
  }

  // TODO: Handle parallelism.
  // TODO: Support old features like "pre-partitioned".
  private PRelNode assignRecursiveInternal(PRelNode currentNode, @Nullable PinotDataDistribution parentDistribution) {
    // Step-1: Assign for left input first
    List<PRelNode> newInputs = new ArrayList<>();
    if (!currentNode.getInputs().isEmpty()) {
      newInputs.add(assignRecursiveInternal(currentNode.getInput(0), null));
    }
    RelDistribution relDistribution = coalesceDistribution(currentNode.getRelNode().getTraitSet().getDistribution());
    RelCollation relCollation = coalesceCollation(currentNode.getRelNode().getTraitSet().getCollation());
    if (currentNode.isLeafStage() && !currentNode.isLeafStageBoundary()) {
      Preconditions.checkState(currentNode.getPinotDataDistributionOrThrow().satisfies(relDistribution));
      Preconditions.checkState(currentNode.getPinotDataDistributionOrThrow().satisfies(relCollation));
      // For leaf-stage, everything under the boundary remains as is.
      return currentNode.copy(currentNode.getNodeId(), newInputs, currentNode.getPinotDataDistributionOrThrow());
    }
    // Step-2: Assign to current node. Workers are the same as the one used for the left input.
    PinotDataDistribution inputDataDistribution = newInputs.get(0).getPinotDataDistributionOrThrow();
    final PinotDataDistribution currentNodeDistribution =
        inputDataDistribution.apply(MappingGen.compute(newInputs.get(0).getRelNode(), currentNode.getRelNode(), null));
    // Step-3: Meet distribution constraint and add exchange if required.
    boolean isDistributionSatisfied = currentNodeDistribution.satisfies(relDistribution);
    PRelNode currentNodeExchange = null;
    if (isDistributionSatisfied) {
      if (parentDistribution != null) {
        // currentNode is right sibling of another node, and since workers for the top-level node are already fixed,
        // we need to make sure that the current node's data-distribution aligns with that.
        // e.g. if parent is an inner-join with servers (S0, S1) with 16 partitions of data, then the right join must
        // also have the same servers and number of partitions, merely being hash-distributed is not enough.
        currentNodeExchange = meetParentEnforcedDistributionConstraintNew(currentNode, newInputs.get(0),
            relDistribution, parentDistribution, currentNodeDistribution);
      }
    } else {
      if (parentDistribution == null) {
        currentNodeExchange = meetDistributionConstraint(currentNode, newInputs.get(0), currentNodeDistribution,
            relDistribution);
      } else {
        currentNodeExchange = meetParentEnforcedDistributionConstraintNew(currentNode, newInputs.get(0),
            relDistribution, parentDistribution, currentNodeDistribution);
      }
    }
    // Step-4: Meet ordering requirement on output streams.
    if (currentNodeExchange == null) {
      if (!currentNodeDistribution.satisfies(relCollation)) {
        // Add new identity exchange for sort.
        PinotPhysicalExchange newExchange = new PinotPhysicalExchange(newInputs.get(0).getRelNode(),
            Collections.emptyList(), PinotExchangeDesc.IDENTITY_EXCHANGE, relCollation);
        PinotDataDistribution newDataDistribution = currentNodeDistribution.withCollation(relCollation);
        currentNodeExchange = new PRelNode(_idGenerator.get(), newExchange, newDataDistribution);
      }
    } else {
      if (!relCollation.getKeys().isEmpty()) {
        // Update existing exchange and add sort.
        PinotPhysicalExchange oldExchange = (PinotPhysicalExchange) currentNodeExchange.getRelNode();
        PinotPhysicalExchange newExchange = new PinotPhysicalExchange(oldExchange.getInput(), oldExchange.getKeys(),
            oldExchange.getExchangeStrategy(), relCollation);
        PinotDataDistribution newDataDistribution = currentNodeExchange.getPinotDataDistributionOrThrow();
        currentNodeExchange = new PRelNode(currentNodeExchange.getNodeId(), newExchange,
            newDataDistribution.withCollation(relCollation));
      }
    }
    // Step-5: Assign to other inputs.
    for (int inputIndex = 1; inputIndex < currentNode.getInputs().size(); inputIndex++) {
      newInputs.add(assignRecursiveInternal(currentNode.getInputs().get(inputIndex), currentNodeDistribution));
      if (currentNodeDistribution.getType() == PinotDataDistribution.Type.HASH_PARTITIONED) {
        PinotDataDistribution newPDD = newInputs.get(inputIndex).getPinotDataDistributionOrThrow();
        if (newPDD.getType() == PinotDataDistribution.Type.HASH_PARTITIONED) {
          currentNodeDistribution.getHashDistributionDesc().addAll(newPDD.getHashDistributionDesc());
        }
      }
    }
    // Step-6: Return the correct node. If exchange is added, return that.
    if (currentNodeExchange != null) {
      PRelNode currentNodeWithNewInputs = currentNode.copy(currentNode.getNodeId(), newInputs, currentNodeDistribution);
      currentNodeExchange.addInput(currentNodeWithNewInputs);
      return currentNodeExchange;
    }
    return currentNode.copy(currentNode.getNodeId(), newInputs, currentNodeDistribution);
  }

  /**
   * There's no parent distribution and given distribution is not satisfied with default assignment.
   * <b>Assumption:</b> Workers are already the same for current and the input nodes.
   */
  private PRelNode meetDistributionConstraint(PRelNode currentNode, PRelNode newInput,
      PinotDataDistribution defaultAssignment, RelDistribution distributionConstraint) {
    Preconditions.checkState(!defaultAssignment.satisfies(distributionConstraint),
        "Method should only be called when constraint is not met");
    PinotDataDistribution inputDataDistribution = newInput.getPinotDataDistributionOrThrow();
    if (distributionConstraint.getType() == RelDistribution.Type.BROADCAST_DISTRIBUTED) {
      PinotPhysicalExchange physicalExchange = PinotPhysicalExchange.broadcast(currentNode.getRelNode());
      PinotDataDistribution pinotDataDistribution = new PinotDataDistribution(PinotDataDistribution.Type.BROADCAST,
          inputDataDistribution.getWorkers(), inputDataDistribution.getWorkerHash(), null, null);
      return new PRelNode(_idGenerator.get(), physicalExchange, pinotDataDistribution);
    }
    if (distributionConstraint.getType() == RelDistribution.Type.SINGLETON) {
      PinotPhysicalExchange physicalExchange = PinotPhysicalExchange.singleton(currentNode.getRelNode());
      List<String> newWorkers = inputDataDistribution.getWorkers().subList(0, 1);
      PinotDataDistribution pinotDataDistribution = new PinotDataDistribution(PinotDataDistribution.Type.SINGLETON,
          newWorkers, newWorkers.hashCode(), null, null);
      return new PRelNode(_idGenerator.get(), physicalExchange, pinotDataDistribution);
    }
    if (distributionConstraint.getType() == RelDistribution.Type.HASH_DISTRIBUTED) {
      PinotPhysicalExchange physicalExchange = new PinotPhysicalExchange(currentNode.getRelNode(),
          distributionConstraint.getKeys(), PinotExchangeDesc.PARTITIONING_EXCHANGE);
      PinotDataDistribution.HashDistributionDesc desc = new PinotDataDistribution.HashDistributionDesc(
          distributionConstraint.getKeys(), "murmur", inputDataDistribution.getWorkers().size());
      PinotDataDistribution pinotDataDistribution = new PinotDataDistribution(
          PinotDataDistribution.Type.HASH_PARTITIONED, inputDataDistribution.getWorkers(),
          inputDataDistribution.getWorkerHash(), ImmutableSet.of(desc), null);
      return new PRelNode(_idGenerator.get(), physicalExchange, pinotDataDistribution);
    }
    throw new IllegalStateException("Distribution constraint not met: " + distributionConstraint.getType());
  }

  @Nullable
  private PRelNode meetParentEnforcedDistributionConstraintNew(PRelNode currentNode, PRelNode newInput,
      RelDistribution relDistribution, PinotDataDistribution parentDistribution,
      PinotDataDistribution assumedDistribution) {
    if (relDistribution.getType() == RelDistribution.Type.RANDOM_DISTRIBUTED
        || relDistribution.getType() == RelDistribution.Type.ANY) {
      // Only need to use the parent's workers.
      if (parentDistribution.getWorkerHash() == assumedDistribution.getWorkerHash()) {
        return null;
      }
      // TODO: Can optimize this to reduce fanout by random sub-partitioning.
      List<Integer> distributionKeys = ImmutableList.of(0);
      PinotPhysicalExchange physicalExchange = new PinotPhysicalExchange(currentNode.getRelNode(),
          distributionKeys, PinotExchangeDesc.PARTITIONING_EXCHANGE);
      PinotDataDistribution newDistribution = new PinotDataDistribution(PinotDataDistribution.Type.HASH_PARTITIONED,
          parentDistribution.getWorkers(), parentDistribution.getWorkerHash(),
          ImmutableSet.of(new PinotDataDistribution.HashDistributionDesc(distributionKeys, "murmur",
              parentDistribution.getWorkers().size())), null);
      return new PRelNode(_idGenerator.get(), physicalExchange, newDistribution);
    } else if (relDistribution.getType() == RelDistribution.Type.BROADCAST_DISTRIBUTED) {
      if (assumedDistribution.getType() == PinotDataDistribution.Type.BROADCAST) {
        if (assumedDistribution.getWorkerHash() == parentDistribution.getWorkerHash()) {
          // If workers are same and broadcast already, nothing to do.
          return null;
        }
        // TODO: Add broadcast to broadcast exchange.
        throw new IllegalStateException("Can't do broadcast to broadcast yet");
      }
      if (assumedDistribution.getType() == PinotDataDistribution.Type.SINGLETON) {
        if (assumedDistribution.getWorkerHash() == parentDistribution.getWorkerHash()) {
          // If single worker and workers are same, no exchange necessary.
          return null;
        }
      }
      PinotPhysicalExchange physicalExchange = new PinotPhysicalExchange(currentNode.getRelNode(),
          Collections.emptyList(), PinotExchangeDesc.BROADCAST_EXCHANGE);
      PinotDataDistribution newDistribution = new PinotDataDistribution(PinotDataDistribution.Type.BROADCAST,
          parentDistribution.getWorkers(), parentDistribution.getWorkerHash(), null, null);
      return new PRelNode(_idGenerator.get(), physicalExchange, newDistribution);
    } else if (relDistribution.getType() == RelDistribution.Type.SINGLETON) {
      if (assumedDistribution.getWorkerHash() == parentDistribution.getWorkerHash()) {
        return null;
      }
      PinotPhysicalExchange physicalExchange = new PinotPhysicalExchange(currentNode.getRelNode(),
          Collections.emptyList(), PinotExchangeDesc.SINGLETON_EXCHANGE);
      PinotDataDistribution newDistribution = new PinotDataDistribution(PinotDataDistribution.Type.SINGLETON,
          parentDistribution.getWorkers(), parentDistribution.getWorkerHash(), null, null);
      return new PRelNode(_idGenerator.get(), physicalExchange, newDistribution);
    }
    Preconditions.checkState(relDistribution.getType() == RelDistribution.Type.HASH_DISTRIBUTED);
    if (parentDistribution.getWorkerHash() == assumedDistribution.getWorkerHash()) {
      if (assumedDistribution.getType() == PinotDataDistribution.Type.HASH_PARTITIONED) {
        if (parentDistribution.getType() == PinotDataDistribution.Type.HASH_PARTITIONED) {
          // If parent is also hash distributed already, then we need to match partition count and hash function.
          PinotDataDistribution.HashDistributionDesc parentDesc = parentDistribution.getHashDistributionDesc()
              .iterator().next();
          PinotDataDistribution.HashDistributionDesc currentDesc = assumedDistribution.getHashDistributionDesc()
              .iterator().next();
          int parentNumPartitions = parentDesc.getNumPartitions();
          String parentHash = parentDesc.getHashFunction();
          int currentNumPartitions = currentDesc.getNumPartitions();
          String currentHash = currentDesc.getHashFunction();
          if (parentNumPartitions == currentNumPartitions && parentHash.equals(currentHash)) {
            return null;
          }
        } else {
          return null;
        }
      }
    }
    // Re-partition.
    // TODO: Can do 1:1 mapping still if number of partitions, number of workers and hash function are same.
    // TODO: Can do a lot more here.
    int numberOfPartitions = parentDistribution.getWorkers().size();
    if (parentDistribution.getType() == PinotDataDistribution.Type.HASH_PARTITIONED) {
      numberOfPartitions = parentDistribution.getHashDistributionDesc().iterator().next().getNumPartitions();
    }
    PinotPhysicalExchange pinotPhysicalExchange = new PinotPhysicalExchange(currentNode.getRelNode(),
        relDistribution.getKeys(), PinotExchangeDesc.PARTITIONING_EXCHANGE, null);
    PinotDataDistribution.HashDistributionDesc newDesc = new PinotDataDistribution.HashDistributionDesc(
        relDistribution.getKeys(), "murmur", numberOfPartitions);
    PinotDataDistribution pinotDataDistribution = new PinotDataDistribution(PinotDataDistribution.Type.HASH_PARTITIONED,
        parentDistribution.getWorkers(), parentDistribution.getWorkerHash(), ImmutableSet.of(newDesc), null);
    return new PRelNode(_idGenerator.get(), pinotPhysicalExchange, pinotDataDistribution);
  }

  private RelDistribution coalesceDistribution(@Nullable RelDistribution distribution) {
    return distribution == null ? RelDistributions.ANY : distribution;
  }

  private RelCollation coalesceCollation(@Nullable RelCollation collation) {
    return collation == null ? RelCollations.EMPTY : collation;
  }
}
