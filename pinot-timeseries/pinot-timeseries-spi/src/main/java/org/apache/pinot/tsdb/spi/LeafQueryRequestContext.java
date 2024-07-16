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
package org.apache.pinot.tsdb.spi;

import java.util.List;


public class LeafQueryRequestContext {
  private final long _requestId;
  private final String _brokerId;
  private final boolean _enableTrace;
  private final boolean _enableStreaming;
  private final List<String> _segmentsToQuery;
  private final List<String> _optionalSegments;

  public LeafQueryRequestContext(
      long requestId,
      String brokerId,
      boolean enableTrace,
      boolean enableStreaming,
      List<String> segmentsToQuery,
      List<String> optionalSegments) {
    _requestId = requestId;
    _brokerId = brokerId;
    _enableTrace = enableTrace;
    _enableStreaming = enableStreaming;
    _segmentsToQuery = segmentsToQuery;
    _optionalSegments = optionalSegments;
  }

  public long getRequestId() {
    return _requestId;
  }

  public String getBrokerId() {
    return _brokerId;
  }

  public boolean isEnableTrace() {
    return _enableTrace;
  }

  public boolean isEnableStreaming() {
    return _enableStreaming;
  }

  public List<String> getSegmentsToQuery() {
    return _segmentsToQuery;
  }

  public List<String> getOptionalSegments() {
    return _optionalSegments;
  }
}
