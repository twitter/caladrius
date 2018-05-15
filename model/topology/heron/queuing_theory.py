from typing import Dict, Any

from caladrius.model.topology.base import TopologyModel
from caladrius.metrics.heron.client import HeronMetricsClient
from caladrius.graph.gremlin.client import GremlinClient

class QTTopologyModel(TopologyModel):

    name: str = "queuing_theory_topology_model"

    description: str = "Models the topology as a queuing network"

    def __init__(self, config: Dict[str, Any],
                 metrics_client: HeronMetricsClient,
                 graph_client: GremlinClient) -> None:
        pass
