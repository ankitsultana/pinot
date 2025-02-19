package org.apache.pinot.calcite.rel.logical;

import java.util.List;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Exchange;


public class PinotPhysicalExchange extends Exchange {
  private final List<Integer> _keys;
  private final PinotPhysicalExchangeStrategy _exchangeStrategy;

  protected PinotPhysicalExchange(RelNode input, List<Integer> keys,
      PinotPhysicalExchangeStrategy exchangeStrategy) {
    super(input.getCluster(), RelTraitSet.createEmpty(), input, RelDistributions.ANY);
    _keys = keys;
    _exchangeStrategy = exchangeStrategy;
  }

  @Override
  public Exchange copy(RelTraitSet traitSet, RelNode newInput, RelDistribution newDistribution) {
    throw new IllegalStateException("copy shouldn't be called for PinotPhysicalExchange");
  }

  public List<Integer> getKeys() {
    return _keys;
  }

  public PinotPhysicalExchangeStrategy getExchangeStrategy() {
    return _exchangeStrategy;
  }
}
