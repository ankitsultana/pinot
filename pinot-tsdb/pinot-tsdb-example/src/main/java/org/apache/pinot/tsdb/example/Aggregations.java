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
package org.apache.pinot.tsdb.example;

import org.apache.pinot.tsdb.spi.AggInfo;


public class Aggregations {
  public static final AggInfo PARTIAL_SUM = new AggInfo(AggType.SUM.name(), true);
  public static final AggInfo FINAL_SUM = new AggInfo(AggType.SUM.name(), false);
  public static final AggInfo PARTIAL_MIN = new AggInfo(AggType.MIN.name(), true);
  public static final AggInfo FINAL_MIN = new AggInfo(AggType.MIN.name(), false);
  public static final AggInfo PARTIAL_MAX = new AggInfo(AggType.MAX.name(), true);
  public static final AggInfo FINAL_MAX = new AggInfo(AggType.MAX.name(), false);

  public enum AggType {
    SUM,
    MAX,
    MIN;
  }
}
