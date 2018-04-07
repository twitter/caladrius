import logging

import datetime as dt

from typing import Dict, Union

from caladrius.model.traffic.traffic_model import TrafficModel

LOG: logging.Logger = logging.getLogger(__name__)

class DummyTrafficModel(TrafficModel):

    def predict_traffic(self, topology_id: str, duration: int) -> dict:

        start: dt.datetime = dt.datetime.utcnow()

        LOG.info("Creating dummy traffic prediction for topology: %s over the "
                 "next %d mins", topology_id, duration)

        duration = dt.datetime.utcnow() - start

        dummy_results: Dict[str, Union[str, int]] = {
            'prediction_method': 'average',
            'calculation_time' : duration.total_seconds(),
            'topology_id': topology_id,
            'prediction_duration' : duration,
            'results': {
                'arrival_rate': {
                    'mean' : 150,
                    'median': 129,
                    'max' : 323,
                    'min' : 102
                }
            }
        }

        return dummy_results
