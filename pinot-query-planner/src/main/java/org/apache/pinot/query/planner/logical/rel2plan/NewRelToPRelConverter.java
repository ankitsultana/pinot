package org.apache.pinot.query.planner.logical.rel2plan;

import org.apache.calcite.rel.RelNode;
import org.apache.pinot.query.context.PlannerContext;
import org.apache.pinot.query.planner.logical.rel2plan.workers.BaseWorkerExchangeAssignment;
import org.apache.pinot.query.planner.logical.rel2plan.workers.LiteModeWorkerExchangeAssignment;
import org.apache.pinot.query.planner.logical.rel2plan.workers.WorkerExchangeAssignment;
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
    LeafStageBoundaryComputer leafStageBoundaryComputer = new LeafStageBoundaryComputer();
    leafStageBoundaryComputer.compute(pRelNode);
    // Step-3: Assign workers to leaf stage nodes.
    _leafWorkerAssignment.compute(pRelNode, _requestId);
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
}
