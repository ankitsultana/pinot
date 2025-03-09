package org.apache.pinot.calcite.rel.logical.traits;

import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitDef;


public class PinotExecStrategyTrait implements RelTrait {
  public static final PinotExecStrategyTrait SUB_PLAN = new PinotExecStrategyTrait(Type.SUB_PLAN);
  public static final PinotExecStrategyTrait PIPELINE_BREAKER = new PinotExecStrategyTrait(Type.PIPELINE_BREAKER);
  public static final PinotExecStrategyTrait STREAMING = new PinotExecStrategyTrait(Type.STREAMING);

  private final Type _type;

  PinotExecStrategyTrait(Type type) {
    _type = type;
  }

  @Override
  @SuppressWarnings("rawtypes")
  public RelTraitDef getTraitDef() {
    return PinotExecStrategyTraitDef.INSTANCE;
  }

  @Override
  public boolean satisfies(RelTrait trait) {
    return trait.getTraitDef() == getTraitDef() && ((PinotExecStrategyTrait) trait)._type == _type;
  }

  @Override
  public void register(RelOptPlanner planner) {
  }

  public Type getType() {
    return _type;
  }

  public enum Type {
    SUB_PLAN,
    PIPELINE_BREAKER,
    STREAMING;
  }
}
