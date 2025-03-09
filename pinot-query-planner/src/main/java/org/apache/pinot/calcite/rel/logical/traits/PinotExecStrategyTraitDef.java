package org.apache.pinot.calcite.rel.logical.traits;

import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelNode;
import org.checkerframework.checker.nullness.qual.Nullable;


public class PinotExecStrategyTraitDef extends RelTraitDef<PinotExecStrategyTrait> {
  public static final PinotExecStrategyTraitDef INSTANCE = new PinotExecStrategyTraitDef();

  @Override
  public Class<PinotExecStrategyTrait> getTraitClass() {
    return PinotExecStrategyTrait.class;
  }

  @Override
  public String getSimpleName() {
    return "pinot-exec-strategy";
  }

  @Override
  public @Nullable RelNode convert(RelOptPlanner planner, RelNode rel, PinotExecStrategyTrait toTrait,
      boolean allowInfiniteCostConverters) {
    if (rel.getTraitSet().contains(toTrait)) {
      return rel;
    }
    return rel.copy(rel.getTraitSet().plus(toTrait), rel.getInputs());
  }

  @Override
  public boolean canConvert(RelOptPlanner planner, PinotExecStrategyTrait fromTrait, PinotExecStrategyTrait toTrait) {
    return true;
  }

  @Override
  public PinotExecStrategyTrait getDefault() {
    return PinotExecStrategyTrait.STREAMING;
  }
}
