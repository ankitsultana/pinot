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
