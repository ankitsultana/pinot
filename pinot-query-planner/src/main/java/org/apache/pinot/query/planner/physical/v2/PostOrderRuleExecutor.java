package org.apache.pinot.query.planner.physical.v2;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import org.apache.pinot.query.context.PhysicalPlannerContext;
import org.apache.pinot.query.planner.logical.rel2plan.PRelNode;


public class PostOrderRuleExecutor extends RuleExecutor {
  private final Deque<PRelNode> _parents = new ArrayDeque<>();

  protected PostOrderRuleExecutor() {
  }

  @Override
  public PRelNode execute(PRelNode currentNode, PRelOptRule rule, PhysicalPlannerContext context) {
    _parents.addLast(currentNode);
    List<PRelNode> newInputs = new ArrayList<>();
    try {
      for (PRelNode input : currentNode.getInputs()) {
        newInputs.add(execute(input, rule, context));
      }
    } finally {
      _parents.removeLast();
    }
    currentNode = currentNode.withNewInputs(currentNode.getNodeId(), newInputs,
        currentNode.getPinotDataDistributionOrThrow());
    PRelOptRuleCall call = new PRelOptRuleCall(currentNode, _parents, context);
    if (rule.matches(call)) {
      currentNode = rule.onMatch(call);
    }
    return currentNode;
  }
}
