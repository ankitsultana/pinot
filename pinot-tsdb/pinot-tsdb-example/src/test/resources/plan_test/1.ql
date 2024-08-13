fetch{table="testTable",filter="column1 > 10",ts_column="some_col",ts_unit="seconds",value="1"}
  | max{city_id,merchant_id}
  | transformNull{0}
  | keepLastValue{}
