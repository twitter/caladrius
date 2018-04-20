from caladrius.graph.graph_client import GraphClient

class HeronGraphClient(GraphClient):

    def __init__(self, config: dict) -> None:
        super().__init__(config)


    def get_sources(self, topology_id: str) -> List[str]:

        pass
