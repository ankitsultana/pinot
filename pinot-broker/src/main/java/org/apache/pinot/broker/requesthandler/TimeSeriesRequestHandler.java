package org.apache.pinot.broker.requesthandler;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Preconditions;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import javax.annotation.Nullable;
import javax.ws.rs.core.HttpHeaders;
import org.apache.hc.client5.http.io.HttpClientConnectionManager;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.pinot.broker.api.AccessControl;
import org.apache.pinot.broker.api.RequesterIdentity;
import org.apache.pinot.broker.broker.AccessControlFactory;
import org.apache.pinot.broker.queryquota.QueryQuotaManager;
import org.apache.pinot.broker.routing.BrokerRoutingManager;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.common.response.BrokerResponse;
import org.apache.pinot.common.response.PrometheusResponse;
import org.apache.pinot.common.utils.HumanReadableDuration;
import org.apache.pinot.query.service.dispatch.QueryDispatcher;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.trace.RequestContext;
import org.apache.pinot.sql.parsers.SqlNodeAndOptions;
import org.apache.pinot.tsdb.planner.TimeSeriesQueryEnvironment;
import org.apache.pinot.tsdb.planner.physical.TimeSeriesDispatchablePlan;
import org.apache.pinot.tsdb.spi.RangeTimeSeriesRequest;
import org.apache.pinot.tsdb.spi.TimeSeriesLogicalPlanResult;


public class TimeSeriesRequestHandler extends BaseBrokerRequestHandler {
  private final TimeSeriesQueryEnvironment _queryEnvironment;
  private final QueryDispatcher _queryDispatcher;

  public TimeSeriesRequestHandler(PinotConfiguration config, String brokerId, BrokerRoutingManager routingManager,
      AccessControlFactory accessControlFactory, QueryQuotaManager queryQuotaManager, TableCache tableCache,
      QueryDispatcher queryDispatcher) {
    super(config, brokerId, routingManager, accessControlFactory, queryQuotaManager, tableCache);
    _queryEnvironment = new TimeSeriesQueryEnvironment(config, routingManager, tableCache);
    _queryEnvironment.init(config);
    _queryDispatcher = queryDispatcher;
  }

  @Override
  protected BrokerResponse handleRequest(long requestId, String query, @Nullable SqlNodeAndOptions sqlNodeAndOptions,
      JsonNode request, @Nullable RequesterIdentity requesterIdentity, RequestContext requestContext,
      @Nullable HttpHeaders httpHeaders, AccessControl accessControl)
      throws Exception {
    throw new IllegalArgumentException("Not supported yet");
  }

  @Override
  public void start() {
  }

  @Override
  public void shutDown() {
  }

  @Override
  public PrometheusResponse handleTimeSeriesRequest(JsonNode request, String rawQueryParamString,
      RequestContext requestContext) {
    RangeTimeSeriesRequest timeSeriesRequest = null;
    try {
      timeSeriesRequest = buildRangeTimeSeriesRequest(request, rawQueryParamString);
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
    TimeSeriesLogicalPlanResult logicalPlanResult = _queryEnvironment.buildLogicalPlan(timeSeriesRequest);
    TimeSeriesDispatchablePlan dispatchablePlan = _queryEnvironment.buildPhysicalPlan(requestContext, logicalPlanResult);
    return _queryDispatcher.submitAndGet(requestContext, dispatchablePlan, 15_000L,
        new HashMap<>());
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

  public static RangeTimeSeriesRequest buildRangeTimeSeriesRequest(JsonNode request, String rawQueryParamString)
      throws URISyntaxException {
    List<NameValuePair> pairs = URLEncodedUtils.parse(
        new URI("http://localhost?" + rawQueryParamString), "UTF-8");
    String query = null;
    Long startTs = null;
    Long endTs = null;
    String step = null;
    String timeout = null;
    for (NameValuePair nameValuePair : pairs) {
      switch (nameValuePair.getName()) {
        case "query":
          query = nameValuePair.getValue();
          break;
        case "start":
          startTs = Long.parseLong(nameValuePair.getValue());
          break;
        case "end":
          endTs = Long.parseLong(nameValuePair.getValue());
          break;
        case "step":
          step = nameValuePair.getValue();
          break;
        case "timeout":
          // TODO: Actually use timeout.
          timeout = nameValuePair.getValue();
          break;
      }
    }
    Preconditions.checkNotNull(query, "Query cannot be null");
    Preconditions.checkNotNull(startTs, "Start time cannot be null");
    Preconditions.checkNotNull(endTs, "End time cannot be null");
    return new RangeTimeSeriesRequest("prom",
        query, startTs, endTs, getStepSeconds(step), Duration.ofSeconds(15));
  }

  public static Long getStepSeconds(@Nullable String step) {
    if (step == null) {
      return 10L;
    }
    try {
      return Long.parseLong(step);
    } catch (NumberFormatException ignored) {
    }
    return HumanReadableDuration.valueOf(step).toSeconds();
  }
}
