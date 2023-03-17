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
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.Hintable;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.metadata.BuiltInMetadata;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.commons.collections.CollectionUtils;


public class PinotRelMdParallelism implements MetadataHandler<BuiltInMetadata.Parallelism> {

  public static final RelMetadataProvider SOURCE =
      ReflectiveRelMetadataProvider.reflectiveSource(new PinotRelMdParallelism(),
          BuiltInMetadata.Parallelism.Handler.class);

  @Override
  public MetadataDef<BuiltInMetadata.Parallelism> getDef() {
    return BuiltInMetadata.Parallelism.DEF;
  }

  public Integer splitCount(RelNode rel, RelMetadataQuery mq) {
    if (rel instanceof Hintable) {
      Hintable hintableRel = (Hintable) rel;
      if (CollectionUtils.isNotEmpty(hintableRel.getHints())) {
        List<RelHint> resourceHints = hintableRel.getHints().stream()
            .filter(hint -> hint.hintName.equals("RESOURCE")).collect(Collectors.toList());
        Preconditions.checkState(resourceHints.size() < 2);
        if (resourceHints.size() == 1) {
          return Integer.parseInt(resourceHints.get(0).kvOptions.getOrDefault("parallelism", "-1"));
        }
      }
    }
    return -1;
  }

  public Boolean isPhaseTransition(RelNode rel, RelMetadataQuery mq) {
    return rel instanceof TableScan || rel instanceof Exchange;
  }
}
