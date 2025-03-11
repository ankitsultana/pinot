package org.apache.pinot.query.planner.physical.v2;

public final class RuleExecutors {
  public static final InOrderRuleExecutor IN_ORDER = new InOrderRuleExecutor();
  public static final PostOrderRuleExecutor POST_ORDER = new PostOrderRuleExecutor();

  private RuleExecutors() {
  }
}
