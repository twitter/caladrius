from typing import Dict, Any

from caladrius.model.topology.base import TopologyModel
from caladrius.metrics.heron.client import HeronMetricsClient
from caladrius.graph.gremlin.client import GremlinClient

class QTTopologyModel(TopologyModel):

    name: str = "queuing_theory_topology_model"

    description: str = ("Models the topology as a queuing network and flags "
                        "if back pressure is likely at instances.")

    def predict_current_performanc(self, topology_id: str,
                                   spout_state: Dict[int, Dict[str, float]]
                                  ) -> Dict[str, Any]:
        # Get the service time for all elements
        # metrics_client.get_service_times()

        # Predict Arrival Rates for all elements
        pass
