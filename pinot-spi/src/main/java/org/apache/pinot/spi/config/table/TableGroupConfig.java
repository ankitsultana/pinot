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
package org.apache.pinot.spi.config.table;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.BaseJsonConfig;


@SuppressWarnings("unused")
public class TableGroupConfig extends BaseJsonConfig {
  @JsonPropertyDescription("The name for the table (with type suffix), e.g. \"myTable_OFFLINE\" (mandatory)")
  private final String _id;

  @JsonCreator
  public TableGroupConfig(@JsonProperty(value = "id", required = false) @Nullable String id) {
    if (id == null) {
      id = "";
    }
    _id = id;
  }

  public String getId() {
    return _id;
  }

  @JsonIgnore
  public boolean isSet() {
    return !_id.equals("");
  }

  public static TableGroupConfig ofEmpty() {
    return new TableGroupConfig("");
  }
}
