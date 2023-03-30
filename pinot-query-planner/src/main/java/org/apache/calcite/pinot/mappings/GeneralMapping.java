/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.calcite.pinot.mappings;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.mapping.IntPair;
import org.apache.calcite.util.mapping.Mappings;


// TODO: Extend CoreMappings?
// TODO: Add constraints for non-negative domain.
public class GeneralMapping implements Iterable<IntPair> {
  private final Map<Integer, Set<Integer>> _map = new HashMap<>();
  private final Map<Integer, Set<Integer>> _reverseMap = new HashMap<>();

  public GeneralMapping inverse() {
    return GeneralMapping.of(_reverseMap);
  }

  public boolean add(int source, int target) {
    _reverseMap.computeIfAbsent(target, (x) -> new HashSet<>()).add(source);
    return _map.computeIfAbsent(source, (x) -> new HashSet<>()).add(target);
  }

  public @Nullable Set<Integer> getTargets(int source) {
    return _map.get(source);
  }

  public @Nullable Set<Integer> getSources(int target) {
    return _reverseMap.get(target);
  }

  public int getSourceCount() {
    return _map.size();
  }

  public int getTargetCount() {
    return _reverseMap.size();
  }

  public @Nullable Mappings.TargetMapping asTargetMapping() {
    int size = _map.size();
    // Check if each source maps to exactly 1 target and each target is mapped to at most 1 source
    boolean oneToOne = _map.values().stream().allMatch(x -> x.size() == 1) && _reverseMap.size() == size;
    if (!oneToOne) {
      return null;
    }
    Map<Integer, Integer> reducedMap = new HashMap<>();
    for (IntPair entry : this) {
      reducedMap.put(entry.source, entry.target);
    }
    return MapBasedTargetMapping.of(reducedMap);
  }

  @Override
  public Iterator<IntPair> iterator() {
    List<IntPair> allPairs = new ArrayList<>();
    for (Map.Entry<Integer, Set<Integer>> entry : _map.entrySet()) {
      int source = entry.getKey();
      for (Integer target : entry.getValue()) {
        allPairs.add(IntPair.of(source, target));
      }
    }
    return allPairs.iterator();
  }

  public static GeneralMapping of(Map<Integer, Set<Integer>> map) {
    GeneralMapping result = new GeneralMapping();
    for (Map.Entry<Integer, Set<Integer>> entry : map.entrySet()) {
      int source = entry.getKey();
      for (Integer target : entry.getValue()) {
        result.add(target, source);
      }
    }
    return result;
  }

  public static GeneralMapping of(Mappings.TargetMapping targetMapping) {
    int numSources = targetMapping.size();
    GeneralMapping generalMapping = new GeneralMapping();
    for (int i = 0; i < numSources; i++) {
      int target = targetMapping.getTargetOpt(i);
      generalMapping.add(i, target);
    }
    return generalMapping;
  }

  // TODO: Review this.
  public static @Nullable GeneralMapping infer(Project project) {
    GeneralMapping mapping = new GeneralMapping();
    for (int i = 0; i < project.getProjects().size(); i++) {
      RexNode p = project.getProjects().get(i);
      if (p instanceof RexInputRef) {
        mapping.add(((RexInputRef) p).getIndex(), i);
      } else if (!(p instanceof RexLiteral)) {
        return null;
      }
    }
    return mapping;
  }

  public static GeneralMapping infer(Aggregate aggregate) {
    Preconditions.checkState(aggregate.getGroupSets().size() <= 1, "Grouping sets are not supported");
    GeneralMapping result = new GeneralMapping();
    if (aggregate.getGroupCount() == 0) {
      return result;
    }
    List<Integer> groupSet = aggregate.getGroupSet().asList();
    for (int i = 0; i < groupSet.size(); i++) {
      result.add(groupSet.get(i), i);
    }
    return result;
  }

  public static Pair<GeneralMapping, GeneralMapping> infer(Join join) {
    GeneralMapping leftMapping =
        GeneralMapping.of(new Mappings.IdentityMapping(join.getLeft().getRowType().getFieldCount()));
    GeneralMapping rightMapping = GeneralMapping.of(
        new OffsetTargetMapping(0, leftMapping.getSourceCount(), leftMapping.getSourceCount()));
    return Pair.of(leftMapping, rightMapping);
  }
}
