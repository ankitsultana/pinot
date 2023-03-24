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

import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitDef;


public class PinotRelParallelismTrait implements RelTrait {

  private final Integer _parallelism;

  public PinotRelParallelismTrait(int parallelism) {
    _parallelism = canonicalize(parallelism);
  }

  @Override
  public boolean satisfies(RelTrait trait) {
    if (trait.getTraitDef().equals(PinotRelParallelismTraitDef.INSTANCE)) {
      return ((PinotRelParallelismTrait) trait).getParallelism().equals(_parallelism);
    }
    return false;
  }

  @Override
  public RelTraitDef<PinotRelParallelismTrait> getTraitDef() {
    return PinotRelParallelismTraitDef.INSTANCE;
  }

  @Override
  public void register(RelOptPlanner planner) {
  }

  public Integer getParallelism() {
    return _parallelism;
  }

  private Integer canonicalize(int parallelism) {
    return parallelism <= 0 ? -1 : parallelism;
  }
}
