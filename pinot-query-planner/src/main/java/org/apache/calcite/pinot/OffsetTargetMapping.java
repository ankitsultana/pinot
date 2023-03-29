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

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.calcite.util.mapping.IntPair;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.MappingType;
import org.apache.calcite.util.mapping.Mappings;


public class OffsetTargetMapping implements Mapping, Mappings.TargetMapping {
  private final int _sourceStart;
  private final int _sourceEnd;
  private final int _size;
  private final int _offset;

  public OffsetTargetMapping(int sourceStart, int sourceEnd, int offset) {
    Preconditions.checkState(sourceStart >= 0 && sourceStart + offset >= 0);
    Preconditions.checkState(sourceStart <= sourceEnd);
    _sourceStart = sourceStart;
    _sourceEnd = sourceEnd;
    _size = sourceEnd - sourceStart;
    _offset = offset;
  }

  public OffsetTargetMapping(int size, int offset) {
    this(0, size, offset);
  }

  @Override
  public int getSourceCount() {
    return _size;
  }

  @Override
  public int getSource(int target) {
    int source = getSourceOpt(target);
    if (source == -1) {
      throw new Mappings.NoElementException("");
    }
    return source;
  }

  @Override
  public int getSourceOpt(int target) {
    if (isTargetValid(target)) {
      return target - _offset;
    }
    return -1;
  }

  @Override
  public int getTargetCount() {
    return _size;
  }

  @Override
  public int getTarget(int source) {
    int target = getTargetOpt(source);
    if (target == -1) {
      throw new Mappings.NoElementException("");
    }
    return target;
  }

  @Override
  public MappingType getMappingType() {
    return MappingType.FUNCTION;
  }

  @Override
  public boolean isIdentity() {
    return _offset == 0;
  }

  @Override
  public void clear() {
  }

  @Override
  public int size() {
    return _size;
  }

  @Override
  public int getTargetOpt(int source) {
    if (isSourceValid(source)) {
      return source + _offset;
    }
    return -1;
  }

  @Override
  public void set(int source, int target) {
    throw new UnsupportedOperationException("Method not implemented");
  }

  @Override
  public Mapping inverse() {
    return new OffsetTargetMapping(_sourceStart + _offset, _sourceEnd + _offset, -_offset);
  }

  @Override
  public Iterator<IntPair> iterator() {
    List<IntPair> mapping = new ArrayList<>(_size);
    for (int source = 0; source < _size; source++) {
      mapping.add(IntPair.of(source, source + _offset));
    }
    return mapping.iterator();
  }

  private boolean isSourceValid(int source) {
    return source >= _sourceStart && source < _sourceEnd;
  }

  private boolean isTargetValid(int target) {
    return isSourceValid(target - _offset);
  }
}
