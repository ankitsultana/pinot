package org.apache.calcite.pinot;

import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelNode;
import org.checkerframework.checker.nullness.qual.Nullable;


public class PinotRelDistributionTraitDef extends RelTraitDef<PinotRelDistribution> {
  public static final PinotRelDistributionTraitDef INSTANCE = new PinotRelDistributionTraitDef();

  private PinotRelDistributionTraitDef() {
  }

  @Override
  public Class<PinotRelDistribution> getTraitClass() {
    return PinotRelDistribution.class;
  }

  @Override
  public String getSimpleName() {
    return "pinot-dist";
  }

  @Override
  public @Nullable RelNode convert(RelOptPlanner planner, RelNode rel, PinotRelDistribution toTrait,
      boolean allowInfiniteCostConverters) {
    throw new UnsupportedOperationException("Pinot doesn't support VolcanoPlanner at the moment.");
  }

  @Override
  public boolean canConvert(RelOptPlanner planner, PinotRelDistribution fromTrait, PinotRelDistribution toTrait) {
    throw new UnsupportedOperationException("Pinot doesn't support VolcanoPlanner at the moment.");
  }

  @Override
  public PinotRelDistribution getDefault() {
    return PinotRelDistribution.ANY;
  }
}
