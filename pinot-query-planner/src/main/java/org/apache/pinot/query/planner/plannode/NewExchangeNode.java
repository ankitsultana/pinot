package org.apache.pinot.query.planner.plannode;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.pinot.calcite.rel.logical.PinotRelExchangeType;
import org.apache.pinot.common.utils.DataSchema;


public class NewExchangeNode extends BasePlanNode {
  private final PinotRelExchangeType _exchangeType;
  private final RelDistribution.Type _distributionType;
  private final List<Integer> _keys;
  private final List<RelFieldCollation> _collations;
  private final boolean _sortOnSender;
  private final boolean _sortOnReceiver;
  // Table names should be set for SUB_PLAN exchange type.
  private final Set<String> _tableNames;

  public NewExchangeNode(int stageId, DataSchema dataSchema, List<PlanNode> inputs, PinotRelExchangeType exchangeType,
      RelDistribution.Type distributionType, @Nullable List<Integer> keys,
      @Nullable List<RelFieldCollation> collations, boolean sortOnSender, boolean sortOnReceiver,
      @Nullable Set<String> tableNames) {
    super(stageId, dataSchema, null, inputs);
    _exchangeType = exchangeType;
    _distributionType = distributionType;
    _keys = keys;
    _collations = collations;
    _sortOnSender = sortOnSender;
    _sortOnReceiver = sortOnReceiver;
    _tableNames = tableNames;
  }

  public PinotRelExchangeType getExchangeType() {
    return _exchangeType;
  }

  public RelDistribution.Type getDistributionType() {
    return _distributionType;
  }

  @Nullable
  public List<Integer> getKeys() {
    return _keys;
  }

  @Nullable
  public List<RelFieldCollation> getCollations() {
    return _collations;
  }

  public boolean isSortOnSender() {
    return _sortOnSender;
  }

  public boolean isSortOnReceiver() {
    return _sortOnReceiver;
  }

  @Nullable
  public Set<String> getTableNames() {
    return _tableNames;
  }

  @Override
  public String explain() {
    return "NEW_EXCHANGE";
  }

  @Override
  public <T, C> T visit(PlanNodeVisitor<T, C> visitor, C context) {
    return visitor.visitNewExchange(this, context);
  }

  @Override
  public PlanNode withInputs(List<PlanNode> inputs) {
    return new NewExchangeNode(_stageId, _dataSchema, inputs, _exchangeType, _distributionType, _keys, _prePartitioned,
        _collations, _sortOnSender, _sortOnReceiver, _tableNames);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof NewExchangeNode)) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    NewExchangeNode that = (NewExchangeNode) o;
    return _sortOnSender == that._sortOnSender && _sortOnReceiver == that._sortOnReceiver
        && _exchangeType == that._exchangeType && _distributionType == that._distributionType
        && Objects.equals(_keys, that._keys) && Objects.equals(_collations, that._collations)
        && Objects.equals(_tableNames, that._tableNames);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), _exchangeType, _distributionType, _keys, _sortOnSender, _sortOnReceiver,
        _collations, _tableNames);
  }
}
