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

import java.util.Map;
import org.apache.pinot.common.response.ProcessingException;


/**
 * For multi-stage we would prefer to return BaseResultsBlock from ServerQueryExecutorV1Impl, and directly convert
 * that to a TransferableBlock later. You will see methods for returning both BaseResultsBlock and DataTable in
 * ServerQueryExecutorV1Impl. This "Bridge" interface essentially allows to re-use most of the code in
 * ServerQueryExecutorV1Impl for both multi-stage and the legacy query engine.
 *
 * <p>
 *   TODO: Refactor ServerQueryExecutorV1Impl so the timing and metadata calls are consolidated and we don't need this.
 * </p>
 */
public interface DataTableBridge {

  void addException(ProcessingException processingException);

  /**
   * Modifying the returned map may not modify the exceptions associated with the object. Use
   * {@link DataTableBridge#addException} instead.
   */
  Map<Integer, String> getExceptions();

  Map<String, String> getMetadata();
}
