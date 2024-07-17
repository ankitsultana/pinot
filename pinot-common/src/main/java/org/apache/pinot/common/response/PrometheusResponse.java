package org.apache.pinot.common.response;

import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;


public class PrometheusResponse {
  private String _status;
  private Data _data;

  public static class Data {
    private String _resultType;
    private List<Result> _result;
  }

  public static class Result {
    private Map<String, String> _metric;
    private List<Pair<Double, String>> _value;
  }
}
