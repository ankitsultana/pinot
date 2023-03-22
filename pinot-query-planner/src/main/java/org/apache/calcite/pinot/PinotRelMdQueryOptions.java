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

import java.util.Map;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.Metadata;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;


public class PinotRelMdQueryOptions implements MetadataHandler<PinotRelMdQueryOptions.PinotQueryOptions> {

  public static final RelMetadataProvider SOURCE =
      ReflectiveRelMetadataProvider.reflectiveSource(new PinotRelMdQueryOptions(),
          PinotQueryOptions.Handler.class);

  @Override
  public MetadataDef<PinotQueryOptions> getDef() {
    return PinotQueryOptions.DEF;
  }

  interface PinotQueryOptions extends Metadata {
    MetadataDef<PinotQueryOptions> DEF = MetadataDef.of(PinotQueryOptions.class,
        PinotQueryOptions.Handler.class, Types.lookupMethod(PinotQueryOptions.class, "getQueryOptions"));

    Map<String, String> getQueryOptions();

    /** Handler API. */
    interface Handler extends MetadataHandler<PinotQueryOptions> {
      Map<String, String> getQueryOptions(RelNode r, RelMetadataQuery mq);

      @Override default MetadataDef<PinotQueryOptions> getDef() {
        return DEF;
      }
    }
  }
}
