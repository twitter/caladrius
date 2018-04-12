""" This module contains classes and methods for modelling traffic based on
summaries of historic spout emission metrics. """

import pandas as pd

from caladrius.model.traffic.traffic_model import TrafficModel

class HistoricTrafficModel(TrafficModel):

    def predict_traffic(self, topology_id: str, duration: int) -> dict:

        spouts: List[str] = self.graph.get_sources(topology_id)

        spout_emissions: pd.DataFrame = \
            self.metrics.get_component_emissions(topology_id, spouts, start,
                                                 end)

