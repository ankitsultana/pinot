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

import io.grpc.ManagedChannel;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.pinot.core.common.datablock.BaseDataBlock;
import org.apache.pinot.query.mailbox.channel.ChannelManager;
import org.apache.pinot.query.mailbox.channel.PassThroughChannel;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * GRPC-based implementation of {@link MailboxService}.
 *
 * <p>It maintains a collection of connected mailbox servers and clients to remote hosts. All indexed by the
 * mailboxID in the format of: <code>"jobId:partitionKey:senderHost:senderPort:receiverHost:receiverPort"</code>
 *
 * <p>Connections are established/initiated from the sender side and only tier-down from the sender side as well.
 * In the event of exception or timed out, the connection is cloased based on a mutually agreed upon timeout period
 * after the last successful message sent/received.
 *
 * <p>Noted that:
 * <ul>
 *   <li>the latter part of the mailboxID consist of the channelID.</li>
 *   <li>the job_id should be uniquely identifying a send/receving pair, for example if one bundle job requires
 *   to open 2 mailboxes, they should use {job_id}_1 and {job_id}_2 to distinguish the 2 different mailbox.</li>
 * </ul>
 */
public class GrpcMailboxService implements MailboxService<BaseDataBlock> {
  private static final Logger LOGGER = LoggerFactory.getLogger(GrpcMailboxService.class);
  // channel manager
  private static final int CAPACITY = 100;
  private final ChannelManager _channelManager;
  private final String _hostname;
  private final int _mailboxPort;

  // maintaining a list of registered mailboxes.
  private final ArrayBlockingQueue<CleanableEntity> _mailboxQueue = new ArrayBlockingQueue<>(100000);
  private final ArrayBlockingQueue<CleanableEntity> _channelQueue = new ArrayBlockingQueue<>(100000);
  private final ConcurrentHashMap<String, ReceivingMailbox<BaseDataBlock>> _receivingMailboxMap =
      new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, SendingMailbox<BaseDataBlock>> _sendingMailboxMap =
      new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, PassThroughChannel<BaseDataBlock>> _channelMap =
      new ConcurrentHashMap<>();

  public GrpcMailboxService(String hostname, int mailboxPort, PinotConfiguration extraConfig) {
    _hostname = hostname;
    _mailboxPort = mailboxPort;
    _channelManager = new ChannelManager(this, extraConfig);
    Thread t = new Thread(() -> {
      while (true) {
        try {
          Thread.sleep(5000);
          LOGGER.info("Size of channelQueue={} and mailboxQueue={}", _channelQueue.size(), _mailboxQueue.size());
          CleanableEntity entity = _mailboxQueue.peek();
          if (entity != null && entity._ts + 1000 * 600 < System.currentTimeMillis()) {
            _mailboxQueue.poll();
            _receivingMailboxMap.remove(entity._id);
            _sendingMailboxMap.remove(entity._id);
          }
          entity = _channelQueue.peek();
          if (entity != null && entity._ts + 1000 * 600 < System.currentTimeMillis()) {
            _channelQueue.poll();
            _channelMap.remove(entity._id);
          }
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    });
    t.setName("Mailbox leak cleaner");
    t.setDaemon(true);
    t.start();
  }

  @Override
  public void start() {
    _channelManager.init();
  }

  @Override
  public void shutdown() {
    _channelManager.shutdown();
  }

  @Override
  public String getHostname() {
    return _hostname;
  }

  @Override
  public int getMailboxPort() {
    return _mailboxPort;
  }

  /**
   * Register a mailbox, mailbox needs to be registered before use.
   * @param mailboxId the id of the mailbox.
   */
  public SendingMailbox<BaseDataBlock> getSendingMailbox(MailboxIdentifier mailboxId) {
    String mailId = mailboxId.toString();
    _mailboxQueue.offer(new CleanableEntity(mailId, System.currentTimeMillis()));
    if (mailboxId.isLocal()) {
      _channelMap.computeIfAbsent(mailId,
          (mId) -> new PassThroughChannel<>(new ArrayBlockingQueue<>(CAPACITY), _hostname, _mailboxPort));
      _channelQueue.offer(new CleanableEntity(mailId, System.currentTimeMillis()));
      return _sendingMailboxMap.computeIfAbsent(mailId, (mId) -> new PassThroughSendingMailbox(
          mId, _channelMap.get(mailId)));
    }
    return _sendingMailboxMap.computeIfAbsent(mailboxId.toString(), (mId) -> new GrpcSendingMailbox(mId, this));
  }

  /**
   * Register a mailbox, mailbox needs to be registered before use.
   * @param mailboxId the id of the mailbox.
   */
  public ReceivingMailbox<BaseDataBlock> getReceivingMailbox(MailboxIdentifier mailboxId) {
    String mailId = mailboxId.toString();
    _mailboxQueue.offer(new CleanableEntity(mailId, System.currentTimeMillis()));
    if (mailboxId.isLocal()) {
      _channelMap.computeIfAbsent(mailId,
          (mId) -> new PassThroughChannel<>(new ArrayBlockingQueue<>(CAPACITY), _hostname, _mailboxPort));
      _channelQueue.offer(new CleanableEntity(mailId, System.currentTimeMillis()));
      return _receivingMailboxMap.computeIfAbsent(mailId, (mId) -> new PassThroughReceivingMailbox(mId,
          _channelMap.get(mId)));
    }
    return _receivingMailboxMap.computeIfAbsent(mailId, (mId) -> new GrpcReceivingMailbox(mId, this));
  }

  public ManagedChannel getChannel(String mailboxId) {
    return _channelManager.getChannel(Utils.constructChannelId(mailboxId));
  }

  static class CleanableEntity {
    protected String _id;
    protected long _ts;

    public CleanableEntity(String id, long ts) {
      _id = id;
      _ts = ts;
    }
  }
}
