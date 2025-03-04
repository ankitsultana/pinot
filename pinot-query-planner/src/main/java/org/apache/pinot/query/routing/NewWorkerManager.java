package org.apache.pinot.query.routing;

import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.core.routing.RoutingManager;
import org.apache.pinot.query.planner.PlanFragment;
import org.apache.pinot.query.planner.physical.DispatchablePlanContext;
import org.apache.pinot.query.planner.physical.DispatchablePlanMetadata;
import org.apache.pinot.query.planner.plannode.NewExchangeNode;
import org.apache.pinot.query.planner.plannode.PlanNode;


/**
 * For each fragment, use any non-exchange node and set workers.
 */
public class NewWorkerManager {
  private final String _hostName;
  private final int _port;
  private final Map<String, QueryServerInstance> _queryServerInstanceMap;

  public NewWorkerManager(String hostName, int port, Map<String, QueryServerInstance> instanceIdToServer) {
    _hostName = hostName;
    _port = port;
    _queryServerInstanceMap = instanceIdToServer;
  }

  public void assignWorkers(PlanFragment rootFragment, DispatchablePlanContext context) {
    DispatchablePlanMetadata metadata = context.getDispatchablePlanMetadataMap().get(0);
    metadata.setWorkerIdToServerInstanceMap(Collections.singletonMap(0, new QueryServerInstance(_hostName, _port,
        _port)));
    for (PlanFragment child : rootFragment.getChildren()) {
      assignWorkersToNonRootFragment(child, context);
    }
  }

  private void assignWorkersToNonRootFragment(PlanFragment nonRootFragment, DispatchablePlanContext context) {
    PlanNode topLevelNode = nonRootFragment.getFragmentRoot();
    while (!(topLevelNode instanceof NewExchangeNode)) {
      Preconditions.checkState(!topLevelNode.getInputs().isEmpty(), "");
      topLevelNode = topLevelNode.getInputs().get(0);
    }
    int fragmentId = nonRootFragment.getFragmentId();
    context.getDispatchablePlanMetadataMap().get(fragmentId).setWorkerIdToServerInstanceMap(
        getWorkerIdToServerInstanceMap(topLevelNode.));
  }

  private Map<Integer, QueryServerInstance> getWorkerIdToServerInstanceMap(List<String> workers) {
    Map<Integer, QueryServerInstance> result = new HashMap<>();
    for (int workerId  = 0; workerId < workers.size(); workerId++) {
      result.put(workerId, _queryServerInstanceMap.get(workers.get(workerId).split("@")[0]));
    }
    return result;
  }
}
