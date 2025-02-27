package org.apache.pinot.query.planner.logical.rel2plan;

import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.util.Permutation;
import org.apache.commons.collections4.CollectionUtils;


public class MappingGen {
  private MappingGen() {
  }

  /**
   * Source to destination mapping.
   */
  @Nullable
  public static Map<Integer, Integer> compute(RelNode source, RelNode destination, @Nullable List<RelNode> leadingSiblings) {
    if (source instanceof Exchange) {
      return compute(source.getInput(0), destination, leadingSiblings);
    } else if (destination instanceof Project) {
      Project project = (Project) destination;
      Permutation permutation = project.getPermutation();
      if (permutation == null) {
        return null;
      }
      Map<Integer, Integer> result = createEmptyMap(source.getRowType().getFieldCount());
      for (int i = 0; i < result.size(); i++) {
        result.put(i, permutation.getTargetOpt(i));
      }
      return result;
    } else if (destination instanceof Window) {
      Window window = (Window) destination;
      Map<Integer, Integer> result = createEmptyMap(source.getRowType().getFieldCount());
      Preconditions.checkState(window.groups.size() <= 1, "Support at most 1 grouping set");
      for (int i = 0; i < window.groups.size(); i++) {
        Window.Group group = window.groups.get(i);
        List<Integer> groupKeys = group.keys.asList();
        for (int j = 0; j < groupKeys.size(); j++) {
          result.put(groupKeys.get(j), j);
        }
      }
      return result;
    } else if (destination instanceof Aggregate) {
      Aggregate aggregate = (Aggregate) destination;
      Map<Integer, Integer> result = createEmptyMap(source.getRowType().getFieldCount());
      List<Integer> groupSet = aggregate.getGroupSet().asList();
      for (int j = 0; j < groupSet.size(); j++) {
        result.put(groupSet.get(j), j);
      }
      return result;
    } else if (destination instanceof Join) {
      if (CollectionUtils.isEmpty(leadingSiblings)) {
        return createIdentityMap(source.getRowType().getFieldCount());
      }
      Preconditions.checkState(leadingSiblings.size() == 1, "At most two nodes allowed in join right now");
      int leftFieldCount = leadingSiblings.get(0).getRowType().getFieldCount();
      Map<Integer, Integer> result = createEmptyMap(source.getRowType().getFieldCount());
      for (int i = 0; i < result.size(); i++) {
        result.put(i, i + leftFieldCount);
      }
      return result;
    } else if (destination instanceof Filter) {
      return createIdentityMap(source.getRowType().getFieldCount());
    } else if (destination instanceof TableScan) {
      throw new IllegalStateException("Found destination as TableScan");
    } else if (destination instanceof Values) {
      throw new IllegalStateException("");
    } else if (destination instanceof Sort) {
      return createIdentityMap(source.getRowType().getFieldCount());
    } else if (destination instanceof SetOp) {
      SetOp setOp = (SetOp) destination;
      Preconditions.checkState(setOp.isHomogeneous(true), "Only homogenous set-op inputs supported right now");
      // minus, union, intersect
      return createIdentityMap(source.getRowType().getFieldCount());
    }
    throw new IllegalStateException("Unknown node type: " + destination.getClass());
  }

  private static Map<Integer, Integer> createIdentityMap(int size) {
    Map<Integer, Integer> result = new HashMap<>();
    for (int i = 0; i < size; i++) {
      result.put(i, i);
    }
    return result;
  }

  private static Map<Integer, Integer> createEmptyMap(int size) {
    Map<Integer, Integer> result = new HashMap<>();
    for (int i = 0; i < size; i++) {
      result.put(i, -1);
    }
    return result;
  }
}
