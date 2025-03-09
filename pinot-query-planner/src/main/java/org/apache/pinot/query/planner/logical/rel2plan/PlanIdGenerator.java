package org.apache.pinot.query.planner.logical.rel2plan;

import java.util.function.Supplier;


public class PlanIdGenerator implements Supplier<Integer> {
  private int _id = 0;

  @Override
  public Integer get() {
    return _id++;
  }
}
