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
package org.apache.pinot.calcite.rel;

public enum PinotExchangeDesc {
  /**
   * Worker-ID with value x, sends data to Worker-ID with value x.
   */
  IDENTITY_EXCHANGE(""),
  /**
   * Worker-ID x will partition the outgoing stream based on the receiving node's {@link PinotDataDistribution}.
   */
  PARTITIONING_EXCHANGE(""),
  /**
   * 1-to-1 but permutation.
   */
  PERMUTATION_EXCHANGE(""),
  /**
   * Worker-ID x will sub-partition: i.e. divide the stream locally within the server. This could be done either via
   * a hash-function, via a round-robin algorithm, or something else.
   */
  SUB_PARTITIONING_HASH_EXCHANGE("murmur"),
  /**
   * Same as above but records are sub-partitioned in a round-robin way.
   */
  SUB_PARTITIONING_RR_EXCHANGE(""),
  /**
   * Each worker will send data to all receiving workers.
   */
  BROADCAST_EXCHANGE("");

  private final String _metadata;

  PinotExchangeDesc(String metadata) {
    _metadata = metadata;
  }
}
