package org.apache.pinot.common.response;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Map;


public class PrometheusResponse {
  private String _status;
  private Data _data;

  @JsonCreator
  public PrometheusResponse(@JsonProperty("status") String status, @JsonProperty("data") Data data) {
    _status = status;
    _data = data;
  }

  public String getStatus() {
    return _status;
  }

  public Data getData() {
    return _data;
  }

  public static class Data {
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
