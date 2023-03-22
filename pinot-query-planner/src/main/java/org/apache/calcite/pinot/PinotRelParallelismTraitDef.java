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
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelNode;
import org.checkerframework.checker.nullness.qual.Nullable;


public class PinotRelParallelismTraitDef extends RelTraitDef<PinotRelParallelismTrait> {
  public static final PinotRelParallelismTraitDef INSTANCE = new PinotRelParallelismTraitDef();

  private PinotRelParallelismTraitDef() {
  }

  @Override
  public Class<PinotRelParallelismTrait> getTraitClass() {
    return PinotRelParallelismTrait.class;
  }

  @Override
  public String getSimpleName() {
    return "pinot-parallelism";
  }

  @Override
  public @Nullable RelNode convert(RelOptPlanner planner, RelNode rel, PinotRelParallelismTrait toTrait,
      boolean allowInfiniteCostConverters) {
    throw new UnsupportedOperationException("Pinot doesn't support VolcanoPlanner at the moment.");
  }

  @Override
  public boolean canConvert(RelOptPlanner planner, PinotRelParallelismTrait fromTrait,
      PinotRelParallelismTrait toTrait) {
    throw new UnsupportedOperationException("Pinot doesn't support VolcanoPlanner at the moment.");
  }

  @Override
  public PinotRelParallelismTrait getDefault() {
    return null;
  }
}
