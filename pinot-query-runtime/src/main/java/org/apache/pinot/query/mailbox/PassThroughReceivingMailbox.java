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
package org.apache.pinot.query.mailbox;

import java.util.concurrent.TimeUnit;
import org.apache.pinot.core.common.datablock.BaseDataBlock;
import org.apache.pinot.query.mailbox.channel.PassThroughChannel;


public class PassThroughReceivingMailbox implements ReceivingMailbox<BaseDataBlock> {
  private final String _mailboxId;
  private final PassThroughChannel<BaseDataBlock> _channel;

  public PassThroughReceivingMailbox(String mailboxId, PassThroughChannel<BaseDataBlock> channel) {
    _mailboxId = mailboxId;
    _channel = channel;
  }

  @Override
  public String getMailboxId() {
    return _mailboxId;
  }

  @Override
  public BaseDataBlock receive()
      throws Exception {
    BaseDataBlock baseDataBlock = _channel.getChannel().poll(120, TimeUnit.SECONDS);
    if (baseDataBlock == null) {
      throw new RuntimeException(String.format("Timed out waiting for data block on mailbox=%s", _mailboxId));
    }
    return baseDataBlock;
  }

  @Override
  public boolean isInitialized() {
    return true;
  }

  @Override
  public boolean isClosed() {
    return _channel.isCompleted() && _channel.getChannel().size() == 0;
  }
}
