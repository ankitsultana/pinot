package org.apache.pinot.broker.requesthandler;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Map;
import java.util.concurrent.Executor;
import javax.annotation.Nullable;
import javax.ws.rs.core.HttpHeaders;
import org.apache.hc.client5.http.io.HttpClientConnectionManager;
import org.apache.pinot.broker.api.AccessControl;
import org.apache.pinot.broker.api.RequesterIdentity;
import org.apache.pinot.broker.broker.AccessControlFactory;
import org.apache.pinot.broker.queryquota.QueryQuotaManager;
import org.apache.pinot.broker.routing.BrokerRoutingManager;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.common.response.BrokerResponse;
import org.apache.pinot.common.response.PrometheusResponse;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.trace.RequestContext;
import org.apache.pinot.sql.parsers.SqlNodeAndOptions;
import org.apache.pinot.tsdb.planner.TimeSeriesQueryEnvironment;


public class TimeSeriesRequestHandler extends BaseBrokerRequestHandler {
  private final TimeSeriesQueryEnvironment _queryEnvironment;

  public TimeSeriesRequestHandler(PinotConfiguration config, String brokerId, BrokerRoutingManager routingManager,
      AccessControlFactory accessControlFactory, QueryQuotaManager queryQuotaManager, TableCache tableCache) {
    super(config, brokerId, routingManager, accessControlFactory, queryQuotaManager, tableCache);
    _queryEnvironment = new TimeSeriesQueryEnvironment(routingManager, tableCache);
  }

  @Override
  protected BrokerResponse handleRequest(long requestId, String query, @Nullable SqlNodeAndOptions sqlNodeAndOptions,
      JsonNode request, @Nullable RequesterIdentity requesterIdentity, RequestContext requestContext,
      @Nullable HttpHeaders httpHeaders, AccessControl accessControl)
      throws Exception {
    _queryEnvironment.buildLogicalPlan()
  }

  @Override
  public void start() {
  }

  @Override
  public void shutDown() {
  }

  @Override
  public PrometheusResponse handleTimeSeriesRequest(JsonNode request, String rawQueryParamString) {
    return null;
  }

  @Override
  public Map<Long, String> getRunningQueries() {
    return Map.of();
  }

  @Override
  public boolean cancelQuery(long queryId, int timeoutMs, Executor executor, HttpClientConnectionManager connMgr,
      Map<String, Integer> serverResponses)
      throws Exception {
    return false;
  }
}
