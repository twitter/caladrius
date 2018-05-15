from typing import Dict, Any

from caladrius.model.topology.base import TopologyModel
from caladrius.metrics.heron.client import HeronMetricsClient
from caladrius.graph.gremlin.client import GremlinClient

DESCRIPTION: str = "Models the topology as a queuing network"

class QTTopologyModel(TopologyModel):

    def __init__(self, config: Dict[str, Any], metrics_client: HeronMetricsClient,
                 graph_client: GremlinClient) -> None:
        super().__init__(config, metrics_client, graph_client,
                         "queuing_theory_topology_model", DESCRIPTION)

