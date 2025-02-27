package org.apache.pinot.calcite.rel.logical;

import java.util.List;
import javax.annotation.Nullable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Exchange;
import org.apache.pinot.calcite.rel.PinotExchangeDesc;


public class PinotPhysicalExchange extends Exchange {
  private final List<Integer> _keys;
  private final PinotExchangeDesc _exchangeStrategy;
  private final boolean _sortByKeys;
  @Nullable
  private final RelFieldCollation.Direction _direction;

  public PinotPhysicalExchange(RelNode input, List<Integer> keys,
      PinotExchangeDesc exchangeStrategy) {
    this(input, keys, exchangeStrategy, false, null);
  }

  public PinotPhysicalExchange(RelNode input, List<Integer> keys, PinotExchangeDesc desc, boolean sortByKeys, RelFieldCollation.Direction direction) {
    super(input.getCluster(), RelTraitSet.createEmpty(), input, RelDistributions.ANY);
    _keys = keys;
    _exchangeStrategy = desc;
    _sortByKeys = sortByKeys;
    _direction = direction;
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

  @Nullable
  public RelFieldCollation.Direction getDirection() {
    return _direction;
  }
}
