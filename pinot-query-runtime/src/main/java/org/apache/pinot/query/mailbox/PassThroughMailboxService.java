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

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.pinot.common.proto.Mailbox;
import org.apache.pinot.query.mailbox.channel.PassThroughChannel;


public class PassThroughMailboxService implements MailboxService<Mailbox.MailboxContent> {
  public static final int MAILBOX_TIMEOUT_SECONDS = 120;
  private final ConcurrentHashMap<String, PassThroughReceivingMailbox> _receivingMailboxMap =
      new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, PassThroughSendingMailbox> _sendingMailboxMap =
      new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, PassThroughChannel<Mailbox.MailboxContent>> _channelMap =
      new ConcurrentHashMap<>();
  private static final int CAPACITY = 50;
  private String _hostname;
  private int _port;

  public PassThroughMailboxService(String hostname, int port) {
    _hostname = hostname;
    _port = port;
  }

  @Override
  public void start() {
  }

  @Override
  public void shutdown() {
  }

  @Override
  public String getHostname() {
    return _hostname;
  }

  @Override
  public int getMailboxPort() {
    return _port;
  }

  @Override
  public PassThroughReceivingMailbox getReceivingMailbox(String mailboxId) {
    _channelMap.computeIfAbsent(mailboxId, (mId) -> new PassThroughChannel<>(new ArrayBlockingQueue<>(CAPACITY),
        _hostname, _port));
    return _receivingMailboxMap.computeIfAbsent(mailboxId, (mId) -> new PassThroughReceivingMailbox(mId,
        _channelMap.get(mId)));
  }

  @Override
  public PassThroughSendingMailbox getSendingMailbox(String mailboxId) {
    _channelMap.computeIfAbsent(mailboxId, (mId) -> new PassThroughChannel<>(new ArrayBlockingQueue<>(CAPACITY),
        _hostname, _port));
    return _sendingMailboxMap.computeIfAbsent(mailboxId, (mId) -> new PassThroughSendingMailbox(mId,
        _channelMap.get(mId)));
  }
}
