package org.apache.pinot.query.planner.physical.v2;

import java.util.Deque;
import org.apache.pinot.query.planner.logical.rel2plan.PRelNode;


public class PRelOptRuleCall {
  public final PRelNode _currentNode;
  public final Deque<PRelNode> _parents;

  public PRelOptRuleCall(PRelNode currentNode, Deque<PRelNode> parents) {
    _currentNode = currentNode;
    _parents = parents;
  }
}
