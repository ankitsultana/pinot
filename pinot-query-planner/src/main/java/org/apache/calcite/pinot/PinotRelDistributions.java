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

import java.util.Collections;
import java.util.List;
import org.apache.calcite.rel.RelDistribution;


public class PinotRelDistributions {
  private static final String DEFAULT_HASH_FUNCTION = "murmur";

  private PinotRelDistributions() {
  }

  public static final PinotRelDistribution ANY = new PinotRelDistribution(
      Collections.emptyList(), null, null, RelDistribution.Type.ANY);

  public static final PinotRelDistribution SINGLETON = new PinotRelDistribution(
      Collections.emptyList(), null, null, RelDistribution.Type.SINGLETON);

  public static final PinotRelDistribution BROADCAST = new PinotRelDistribution(
      Collections.emptyList(), null, null, RelDistribution.Type.BROADCAST_DISTRIBUTED);

  public static final PinotRelDistribution RANDOM = new PinotRelDistribution(
      Collections.emptyList(), null, null, RelDistribution.Type.HASH_DISTRIBUTED);

  public static PinotRelDistribution hash(List<Integer> keys, Integer numPartitions) {
    return new PinotRelDistribution(keys, numPartitions, DEFAULT_HASH_FUNCTION, RelDistribution.Type.HASH_DISTRIBUTED);
  }
}
