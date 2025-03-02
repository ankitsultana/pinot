package org.apache.pinot.calcite.rel.logical;

import java.util.List;
import javax.annotation.Nullable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Exchange;
import org.apache.pinot.calcite.rel.PinotExchangeDesc;


public class PinotPhysicalExchange extends Exchange {
  private final List<Integer> _keys;
  private final PinotExchangeDesc _exchangeStrategy;
  private final RelCollation _collation;

  public PinotPhysicalExchange(RelNode input, List<Integer> keys,
      PinotExchangeDesc exchangeStrategy) {
    this(input, keys, exchangeStrategy, null);
  }

  public PinotPhysicalExchange(RelNode input, List<Integer> keys, PinotExchangeDesc desc, RelCollation collation) {
    super(input.getCluster(), RelTraitSet.createEmpty(), input, RelDistributions.RANDOM_DISTRIBUTED);
    _keys = keys;
    _exchangeStrategy = desc;
    _collation = collation == null ? RelCollations.EMPTY : collation;
  }

  @Override
  public Exchange copy(RelTraitSet traitSet, RelNode newInput, RelDistribution newDistribution) {
    throw new IllegalStateException("copy shouldn't be called for PinotPhysicalExchange");
  }

  public List<Integer> getKeys() {
    return _keys;
  }

  public PinotExchangeDesc getExchangeStrategy() {
    return _exchangeStrategy;
  }

  public RelCollation getCollation() {
    return _collation;
  }

  @Override
  public String getRelTypeName() {
    return String.format("PinotPhysicalExchange(strategy=%s, keys=%s)", _exchangeStrategy, _keys);
  }
}
