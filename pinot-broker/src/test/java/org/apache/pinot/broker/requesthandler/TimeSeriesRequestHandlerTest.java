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
package org.apache.pinot.broker.requesthandler;

import java.net.URISyntaxException;
import java.time.Duration;
import org.apache.pinot.tsdb.spi.RangeTimeSeriesRequest;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class TimeSeriesRequestHandlerTest {
  @Test
  public void testBuildRangeTimeSeriesRequest()
      throws URISyntaxException {
    final String sampleRequest = "end=1723564710&query=fetch%7Btable%3D%22testTable%22%2Cfilter%3D%22column1+%3E+10%22"
        + "%2Cts_column%3D%22some_col%22%2Cts_unit%3D%22seconds%22%2Cvalue%3D%221%22%7D%0A++%7C+max%7Bcity_id%2C"
        + "merchant_id%7D%0A++%7C+transformNull%7B0%7D%0A++%7C+keepLastValue%7B%7D%0A&start=1723543110&step=30";
    RangeTimeSeriesRequest request = TimeSeriesRequestHandler.buildRangeTimeSeriesRequest("example", sampleRequest);
    assertEquals(request.getEngine(), "example");
    assertEquals(request.getStartSeconds(), 1723543110);
    assertEquals(request.getEndSeconds(), 1723564710);
    assertEquals(request.getTimeout(), Duration.ofSeconds(15));
    assertEquals(request.getStepSeconds(), 30);
  }
}
