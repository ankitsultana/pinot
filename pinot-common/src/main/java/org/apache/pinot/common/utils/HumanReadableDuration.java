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
package org.apache.pinot.common.utils;

import java.time.Duration;


public class HumanReadableDuration {
  private HumanReadableDuration() {
  }

  public static Duration fromString(String value) {
    value = value.trim();
    if (value.endsWith("h") || value.endsWith("H")) {
      return Duration.ofHours(Long.parseLong(value.substring(0, value.length() - 1)));
    } else if (value.endsWith("d") || value.endsWith("D")) {
      return Duration.ofDays(Long.parseLong(value.substring(0, value.length() - 1)));
    } else if (value.endsWith("s") || value.endsWith("S")) {
      return Duration.ofSeconds(Long.parseLong(value.substring(0, value.length() - 1)));
    } else if (value.endsWith("m") || value.endsWith("M")) {
      return Duration.ofMinutes(Long.parseLong(value.substring(0, value.length() - 1)));
    } else {
      throw new IllegalArgumentException("Cannot handle duration string: " + value);
    }
  }
}
