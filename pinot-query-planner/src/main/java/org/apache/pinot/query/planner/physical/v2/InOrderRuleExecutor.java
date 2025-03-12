package org.apache.pinot.query.planner.physical.v2;

import com.google.common.collect.ImmutableList;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import org.apache.pinot.query.context.PhysicalPlannerContext;
import org.apache.pinot.query.planner.logical.rel2plan.PRelNode;


public class InOrderRuleExecutor extends RuleExecutor {
  private final Deque<PRelNode> _parents = new ArrayDeque<>();

  InOrderRuleExecutor() {
  }

  public PRelNode execute(PRelNode currentNode, PRelOptRule rule, PhysicalPlannerContext context) {
    if (!currentNode.getInputs().isEmpty()) {
      PRelNode modifiedInput = execute(currentNode.getInput(0), rule, context);
      if (modifiedInput != currentNode.getInput(0)) {
        ImmutableList.Builder<PRelNode> listBuilder = ImmutableList.builder();
        listBuilder.add(modifiedInput);
        listBuilder.addAll(currentNode.getInputs().subList(1, currentNode.getInputs().size()));
        currentNode = currentNode.withNewInputs(currentNode.getNodeId(), listBuilder.build(),
            currentNode.getPinotDataDistributionOrThrow());
      }
    }
    PRelOptRuleCall call = new PRelOptRuleCall(currentNode, _parents, context);
    if (rule.matches(call)) {
      currentNode = rule.onMatch(call);
    }
    List<PRelNode> newInputs = new ArrayList<>(currentNode.getInputs());
    for (int index = 1; index < currentNode.getInputs().size(); index++) {
      newInputs.set(index, execute(currentNode.getInput(index), rule, context));
    }
    currentNode = currentNode.withNewInputs(currentNode.getNodeId(), newInputs,
        currentNode.getPinotDataDistributionOrThrow());
    currentNode = rule.onDone(currentNode);
    return currentNode;
  }
}
