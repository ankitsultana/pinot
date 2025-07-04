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
package org.apache.pinot.broker.querylog;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.RateLimiter;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.broker.requesthandler.BaseSingleStageBrokerRequestHandler.ServerStats;
import org.apache.pinot.common.response.BrokerResponse;
import org.apache.pinot.spi.auth.broker.RequesterIdentity;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.trace.RequestContext;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.spi.utils.CommonConstants.Broker;


/**
 * {@code QueryLogger} is responsible for logging query responses in a configurable
 * fashion. Query logging can be useful to capture production traffic to assist with
 * debugging or regression testing.
 */
@SuppressWarnings("UnstableApiUsage")
public class QueryLogger {

  private static final Logger LOGGER = LoggerFactory.getLogger(QueryLogger.class);
  private static final QueryLogEntry[] QUERY_LOG_ENTRY_VALUES = QueryLogEntry.values();

  private final int _maxQueryLengthToLog;
  private final RateLimiter _logRateLimiter;
  private final boolean _enableIpLogging;
  private final boolean _logBeforeProcessing;
  private final Logger _logger;
  private final RateLimiter _droppedLogRateLimiter;
  private final AtomicLong _numDroppedLogs = new AtomicLong(0L);

  public QueryLogger(PinotConfiguration config) {
    this(RateLimiter.create(config.getProperty(Broker.CONFIG_OF_BROKER_QUERY_LOG_MAX_RATE_PER_SECOND,
            Broker.DEFAULT_BROKER_QUERY_LOG_MAX_RATE_PER_SECOND)),
        config.getProperty(Broker.CONFIG_OF_BROKER_QUERY_LOG_LENGTH, Broker.DEFAULT_BROKER_QUERY_LOG_LENGTH),
        config.getProperty(Broker.CONFIG_OF_BROKER_REQUEST_CLIENT_IP_LOGGING,
            Broker.DEFAULT_BROKER_REQUEST_CLIENT_IP_LOGGING),
        config.getProperty(Broker.CONFIG_OF_BROKER_QUERY_LOG_BEFORE_PROCESSING,
            Broker.DEFAULT_BROKER_QUERY_LOG_BEFORE_PROCESSING), LOGGER, RateLimiter.create(1)
        // log once a second for dropped log count
    );
  }

  @VisibleForTesting
  QueryLogger(RateLimiter logRateLimiter, int maxQueryLengthToLog, boolean enableIpLogging, boolean logBeforeProcessing,
      Logger logger, RateLimiter droppedLogRateLimiter) {
    _logRateLimiter = logRateLimiter;
    _maxQueryLengthToLog = maxQueryLengthToLog;
    _enableIpLogging = enableIpLogging;
    _logger = logger;
    _droppedLogRateLimiter = droppedLogRateLimiter;
    _logBeforeProcessing = logBeforeProcessing;
  }

  public void log(long requestId, String query) {
    if (!_logBeforeProcessing || !checkRateLimiter(null)) {
      return;
    }

    _logger.info("SQL query for request {}: {}", requestId, query);

    tryLogDropped();
  }

  public void log(QueryLogParams params) {
    _logger.debug("Broker Response: {}", params._response);

    if (!checkRateLimiter(params)) {
      return;
    }

    final StringBuilder queryLogBuilder = new StringBuilder();
    for (QueryLogEntry value : QUERY_LOG_ENTRY_VALUES) {
      value.format(queryLogBuilder, this, params);
      queryLogBuilder.append(',');
    }

    // always log the query last - don't add this to the QueryLogEntry enum
    queryLogBuilder.append("query=")
        .append(StringUtils.substring(params._requestContext.getQuery(), 0, _maxQueryLengthToLog));
    _logger.info(queryLogBuilder.toString());

    tryLogDropped();
  }

  private boolean checkRateLimiter(@Nullable QueryLogParams params) {
    boolean allowed = _logRateLimiter.tryAcquire() || shouldForceLog(params);
    if (!allowed) {
      _numDroppedLogs.incrementAndGet();
    }
    return allowed;
  }

  private void tryLogDropped() {
    if (_droppedLogRateLimiter.tryAcquire()) {
      // use getAndSet to 0 so that there will be no race condition between
      // loggers that increment this counter and this thread
      long numDroppedLogsSinceLastLog = _numDroppedLogs.getAndSet(0);
      if (numDroppedLogsSinceLastLog > 0) {
        _logger.warn("{} logs were dropped. (log max rate per second: {})", numDroppedLogsSinceLastLog,
            _logRateLimiter.getRate());
      }
    }
  }

  public int getMaxQueryLengthToLog() {
    return _maxQueryLengthToLog;
  }

  public double getLogRateLimit() {
    return _logRateLimiter.getRate();
  }

  private boolean shouldForceLog(@Nullable QueryLogParams params) {
    if (params == null) {
      return false;
    }
    return params._response.isPartialResult() || params._response.getTimeUsedMs() > TimeUnit.SECONDS.toMillis(1);
  }

  public static class QueryLogParams {
    private final RequestContext _requestContext;
    private final String _table;
    private final BrokerResponse _response;
    private final QueryEngine _queryEngine;
    @Nullable
    private final RequesterIdentity _identity;
    @Nullable
    private final ServerStats _serverStats;

    public QueryLogParams(RequestContext requestContext, String table, BrokerResponse response,
        QueryEngine queryEngine, @Nullable RequesterIdentity identity, @Nullable ServerStats serverStats) {
      _requestContext = requestContext;
      // NOTE: Passing table name separately because table name within request context is always raw table name.
      _table = table;
      _response = response;
      _queryEngine = queryEngine;
      _identity = identity;
      _serverStats = serverStats;
    }

    public enum QueryEngine {
      SINGLE_STAGE("singleStage"),
      MULTI_STAGE("multiStage");

      private final String _name;

      QueryEngine(String name) {
        _name = name;
      }

      private String getName() {
        return _name;
      }
    }
  }

  /**
   * NOTE: please maintain the order of this query log entry enum. If you want to add a new
   * entry, add it to the end of the existing list.
   */
  private enum QueryLogEntry {
    REQUEST_ID("requestId") {
      @Override
      void doFormat(StringBuilder builder, QueryLogger logger, QueryLogParams params) {
        // NOTE: At this moment, request ID is not available at response yet.
        builder.append(params._requestContext.getRequestId());
      }
    },
    TABLE("table") {
      @Override
      void doFormat(StringBuilder builder, QueryLogger logger, QueryLogParams params) {
        builder.append(params._table);
      }
    },
    TIME_MS("timeMs") {
      @Override
      void doFormat(StringBuilder builder, QueryLogger logger, QueryLogParams params) {
        builder.append(params._response.getTimeUsedMs());
      }
    },
    DOCS("docs") {
      @Override
      void doFormat(StringBuilder builder, QueryLogger logger, QueryLogParams params) {
        builder.append(params._response.getNumDocsScanned()).append('/').append(params._response.getTotalDocs());
      }
    },
    ENTRIES("entries") {
      @Override
      void doFormat(StringBuilder builder, QueryLogger logger, QueryLogParams params) {
        builder.append(params._response.getNumEntriesScannedInFilter()).append('/')
            .append(params._response.getNumEntriesScannedPostFilter());
      }
    },
    SEGMENT_INFO("segments(queried/processed/matched/consumingQueried/consumingProcessed/consumingMatched/unavailable)",
        ':') {
      @Override
      void doFormat(StringBuilder builder, QueryLogger logger, QueryLogParams params) {
        builder.append(params._response.getNumSegmentsQueried()).append('/')
            .append(params._response.getNumSegmentsProcessed()).append('/')
            .append(params._response.getNumSegmentsMatched()).append('/')
            .append(params._response.getNumConsumingSegmentsQueried()).append('/')
            .append(params._response.getNumConsumingSegmentsProcessed()).append('/')
            .append(params._response.getNumConsumingSegmentsMatched()).append('/')
            // TODO: Consider adding the number of unavailable segments to the response
            .append(params._requestContext.getNumUnavailableSegments());
      }
    },
    CONSUMING_FRESHNESS_MS("consumingFreshnessTimeMs") {
      @Override
      void doFormat(StringBuilder builder, QueryLogger logger, QueryLogParams params) {
        builder.append(params._response.getMinConsumingFreshnessTimeMs());
      }
    },
    SERVERS("servers") {
      @Override
      void doFormat(StringBuilder builder, QueryLogger logger, QueryLogParams params) {
        builder.append(params._response.getNumServersResponded()).append('/')
            .append(params._response.getNumServersQueried());
      }
    },
    GROUP_LIMIT_REACHED("groupLimitReached") {
      @Override
      void doFormat(StringBuilder builder, QueryLogger logger, QueryLogParams params) {
        builder.append(params._response.isNumGroupsLimitReached());
      }
    },
    GROUP_WARNING_LIMIT_REACHED("groupWarningLimitReached") {
      @Override
      void doFormat(StringBuilder builder, QueryLogger logger, QueryLogParams params) {
        builder.append(params._response.isNumGroupsWarningLimitReached());
      }
    },
    BROKER_REDUCE_TIME_MS("brokerReduceTimeMs") {
      @Override
      void doFormat(StringBuilder builder, QueryLogger logger, QueryLogParams params) {
        builder.append(params._response.getBrokerReduceTimeMs());
      }
    },
    EXCEPTIONS("exceptions") {
      @Override
      void doFormat(StringBuilder builder, QueryLogger logger, QueryLogParams params) {
        builder.append(params._response.getExceptionsSize());
      }
    },
    SERVER_STATS("serverStats") {
      @Override
      void doFormat(StringBuilder builder, QueryLogger logger, QueryLogParams params) {
        if (params._serverStats != null) {
          builder.append(params._serverStats.getServerStats());
        } else {
          builder.append(CommonConstants.UNKNOWN);
        }
      }
    },
    OFFLINE_THREAD_CPU_TIME("offlineThreadCpuTimeNs(total/thread/sysActivity/resSer)", ':') {
      @Override
      void doFormat(StringBuilder builder, QueryLogger logger, QueryLogParams params) {
        builder.append(params._response.getOfflineTotalCpuTimeNs()).append('/')
            .append(params._response.getOfflineThreadCpuTimeNs()).append('/')
            .append(params._response.getOfflineSystemActivitiesCpuTimeNs()).append('/')
            .append(params._response.getOfflineResponseSerializationCpuTimeNs());
      }
    },
    REALTIME_THREAD_CPU_TIME("realtimeThreadCpuTimeNs(total/thread/sysActivity/resSer)", ':') {
      @Override
      void doFormat(StringBuilder builder, QueryLogger logger, QueryLogParams params) {
        builder.append(params._response.getRealtimeTotalCpuTimeNs()).append('/')
            .append(params._response.getRealtimeThreadCpuTimeNs()).append('/')
            .append(params._response.getRealtimeSystemActivitiesCpuTimeNs()).append('/')
            .append(params._response.getRealtimeResponseSerializationCpuTimeNs());
      }
    },
    CLIENT_IP("clientIp") {
      @Override
      void doFormat(StringBuilder builder, QueryLogger logger, QueryLogParams params) {
        if (logger._enableIpLogging && params._identity != null) {
          builder.append(params._identity.getClientIp());
        } else {
          builder.append(CommonConstants.UNKNOWN);
        }
      }
    },
    QUERY_ENGINE("queryEngine") {
      @Override
      void doFormat(StringBuilder builder, QueryLogger logger, QueryLogParams params) {
        builder.append(params._queryEngine.getName());
      }
    },
    OFFLINE_MEM_ALLOCATED_BYTES("offlineMemAllocatedBytes(total/thread/resSer)", ':') {
      @Override
      void doFormat(StringBuilder builder, QueryLogger logger, QueryLogParams params) {
        builder.append(params._response.getOfflineTotalMemAllocatedBytes()).append('/')
            .append(params._response.getOfflineThreadMemAllocatedBytes()).append('/')
            .append(params._response.getOfflineResponseSerMemAllocatedBytes());
      }
    },
    REALTIME_MEM_ALLOCATED_BYTES("realtimeMemAllocatedBytes(total/thread/resSer)", ':') {
      @Override
      void doFormat(StringBuilder builder, QueryLogger logger, QueryLogParams params) {
        builder.append(params._response.getRealtimeTotalMemAllocatedBytes()).append('/')
            .append(params._response.getRealtimeThreadMemAllocatedBytes()).append('/')
            .append(params._response.getRealtimeResponseSerMemAllocatedBytes());
      }
    },
    POOLS("pools") {
      @Override
      void doFormat(StringBuilder builder, QueryLogger logger, QueryLogParams params) {
          builder.append(params._response.getPools());
      }
    };

    public final String _entryName;
    public final char _separator; // backwards compatibility for the entries that use ':' instead of '='

    QueryLogEntry(String entryName) {
      this(entryName, '=');
    }

    QueryLogEntry(String entryName, final char separator) {
      _entryName = entryName;
      _separator = separator;
    }

    abstract void doFormat(StringBuilder builder, QueryLogger logger, QueryLogParams params);

    void format(StringBuilder builder, QueryLogger logger, QueryLogParams params) {
      // use StringBuilder because the compiler will struggle to turn string complicated
      // (as part of a loop) string concatenation into StringBuilder, which is significantly
      // more efficient
      builder.append(_entryName).append(_separator);
      doFormat(builder, logger, params);
    }
  }
}
