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
package org.apache.pinot.common.response;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class PrometheusResponse {
  private String _status;
  private Data _data;
  private String _errorType;
  private String _error;

  @JsonCreator
  public PrometheusResponse(@JsonProperty("status") String status, @JsonProperty("data") Data data,
      @JsonProperty("errorType") String errorType, @JsonProperty("error") String error) {
    _status = status;
    _data = data;
    _errorType = errorType;
    _error = error;
  }

  public String getStatus() {
    return _status;
  }

  public Data getData() {
    return _data;
  }

  public String getErrorType() {
    return _errorType;
  }

  public String getError() {
    return _error;
  }

  public static class Data {
    public static final Data EMPTY = new Data("", new ArrayList<>());
    private String _resultType;
    private List<Value> _result;

    @JsonCreator
    public Data(@JsonProperty("resultType") String resultType, @JsonProperty("result") List<Value> result) {
      _resultType = resultType;
      _result = result;
    }

    public String getResultType() {
      return _resultType;
    }

    public List<Value> getResult() {
      return _result;
    }
  }

  public static class Value {
    private Map<String, String> _metric;
    private Object[][] _values;

    @JsonCreator
    public Value(@JsonProperty("metric") Map<String, String> metric, @JsonProperty("values") Object[][] values) {
      _metric = metric;
      _values = values;
    }

    public Map<String, String> getMetric() {
      return _metric;
    }

    public Object[][] getValues() {
      return _values;
    }
  }
}
