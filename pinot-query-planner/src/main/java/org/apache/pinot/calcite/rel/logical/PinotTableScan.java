package org.apache.pinot.calcite.rel.logical;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.RelHint;


/**
 * Exists as replacement for {@link org.apache.calcite.rel.logical.LogicalTableScan}, because the copy method in it
 * does not allow updating traits.
 */
public class PinotTableScan extends TableScan {
  public PinotTableScan(RelOptCluster cluster, RelTraitSet traitSet, List<RelHint> hints, RelOptTable table) {
    super(cluster, traitSet, hints, table);
  }

  @Override public RelNode withHints(List<RelHint> hintList) {
    return new PinotTableScan(getCluster(), traitSet, hintList, table);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    assert inputs.isEmpty();
    return new PinotTableScan(getCluster(), traitSet, hints, table);
  }
}
