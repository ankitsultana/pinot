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
package org.apache.calcite.pinot;

import java.util.Iterator;
import java.util.Map;
import org.apache.calcite.util.mapping.IntPair;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.MappingType;
import org.apache.calcite.util.mapping.Mappings;


public class MapBasedTargetMapping implements Mappings.TargetMapping {
  private final Map<Integer, Integer> _map;
  private final Integer _targetCount;

  private MapBasedTargetMapping(Map<Integer, Integer> map) {
    _map = map;
    _targetCount = _map.values().size();
  }

  public static MapBasedTargetMapping of(Map<Integer, Integer> map) {
    return new MapBasedTargetMapping(map);
  }

  @Override
  public int getSourceCount() {
    return _map.size();
  }

  @Override
  public int getSourceOpt(int target) {
    for (Map.Entry<Integer, Integer> entry : _map.entrySet()) {
      if (entry.getValue().equals(target)) {
        return entry.getKey();
      }
    }
    throw new IllegalStateException("");
  }

  @Override
  public int getTargetCount() {
    return _targetCount;
  }

  @Override
  public int getTarget(int source) {
    return _map.getOrDefault(source, -1);
  }

  @Override
  public MappingType getMappingType() {
    return null;
  }

  @Override
  public int size() {
    return _map.size();
  }

  @Override
  public int getTargetOpt(int source) {
    return _map.getOrDefault(source, -1);
  }

  @Override
  public void set(int source, int target) {
    _map.put(source, target);
  }

  @Override
  public Mapping inverse() {
    return null;
  }

  @Override
  public Iterator<IntPair> iterator() {
    return null;
  }
}
