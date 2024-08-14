package org.apache.pinot.query.runtime.timeseries.serde;

import com.google.common.collect.ImmutableMap;
import org.apache.pinot.common.response.PrometheusResponse;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class SeriesBlockSerdeUtilsTest {
  @Test
  public void testConvertToPrometheusResponse() {
    SeriesBlockSerialized serialized = new SeriesBlockSerialized(
        new Long[]{1L, 2L, 3L},
        ImmutableMap.of(
            1L,
            new SeriesBlockEntry("series-1",
                ImmutableMap.of("foo", "v1"), new Double[][]{ {1.0, 2.0, 3.0} }),
            2L,
            new SeriesBlockEntry("series-2",
                ImmutableMap.of("bar", "v2"), new Double[][]{ {4.0, 5.0, 6.0} })));
    PrometheusResponse response = SeriesBlockSerdeUtils.convertToPrometheusResponse(serialized);
    assertNotNull(response);
  }
}
