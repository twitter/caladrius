import datetime as dt
import logging
import pandas as pd
from typing import Any, Dict

from caladrius.traffic_provider.trafficprovider import TrafficProvider
from caladrius.graph.gremlin.client import GremlinClient
from caladrius.metrics.heron.client import HeronMetricsClient
from caladrius.model.traffic.heron.base import HeronTrafficModel
from caladrius.model.traffic.heron.prophet import ProphetTrafficModel
from model.topology.heron.helpers import convert_throughput_to_inter_arr_times

LOG: logging.Logger = logging.getLogger(__name__)


class PredictedTraffic(TrafficProvider):
    """ This module takes in user-provided information and uses it to create a
    HeronTraffic Model that can be used to predict and supply arrival rate information
    about traffic in future.
    """

    def __init__(self, metrics_client: HeronMetricsClient, graph_client: GremlinClient,
                 topology_id: str, cluster: str, environ: str, start: [dt.datetime],
                 end: [dt.datetime], traffic_config: Dict[str, Any], **other_kwargs) -> None:

        self.topology_id = topology_id
        self.cluster = cluster
        self.environ = environ
        self.start = start
        self.end = end
        self.kwargs = other_kwargs
        self.graph_client: GremlinClient = graph_client
        self.metrics_client: HeronMetricsClient = metrics_client
        self.arrival_rate = None
        self.inter_arrival_time = None
        self.tuple_arrival = None

        model: HeronTrafficModel = ProphetTrafficModel(traffic_config, self.metrics_client, self.graph_client)

        # this data structure contains data received per instance per second
        self.prediction_results = model.predict_traffic(topology_id, cluster, environ, **other_kwargs)

    def tuple_arrivals(self):
        """This function returns the number of tuples arrived at an instance in a minute"""
        # Note that these are tuple arrival values for future cases and predictions themselves.
        # we shouldn't use them to validate queue sizes.
        if self.arrival_rate is None:
            self.arrival_rates()

        self.tuple_arrival = self.arrival_rate.copy()
        # the following converts the data back to tuple arrivals per minute
        self.tuple_arrival['num-tuples'] = self.arrival_rate['mean_arrival_rate'] * 60 * 1000
        self.tuple_arrival.drop(["mean_arrival_rate"], axis=1)
        return self.tuple_arrival

    def arrival_rates(self) -> pd.DataFrame:
        """This function returns the number of tuples arrived at an instance per ms"""
        df: pd.DataFrame = pd.DataFrame(columns=['task', 'mean_arrival_rate'])
        # this function returns arrival rates as number of tuples that arrive per millisecond
        # as prediction_results data is in seconds, we need to divide to get data for millseconds
        # format --> task  mean_arrival_rate
        mean_rates = self.prediction_results["instances"]["mean"]
        for task, rates in mean_rates.items():
            data_across_streams = 0
            for _, value in rates.items():
                data_across_streams = data_across_streams + value
            df = df.append({'task': task,
                            'mean_arrival_rate': data_across_streams / 1000}, ignore_index=True)
        self.arrival_rate = df
        return df

    def inter_arrival_times(self):
        """This function returns the time between the arrival of two subsequent tuples in ms"""
        # this function returns arrival times between two subsequent tuples in ms
        # task  mean_inter_arrival_time  std_inter_arrival_time
        if self.inter_arrival_time is None:
            if self.tuple_arrival is None:
                self.tuple_arrivals()
            self.inter_arrival_time = convert_throughput_to_inter_arr_times(self.tuple_arrival)

        return self.inter_arrival_time

    def service_times(self):
        """TODO: Also predict service times for all components!"""
        bolt_service_times = self.metrics_client.get_service_times(self.topology_id, self.cluster,
                                                                   self.environ, self.start, self.end, **self.kwargs)
        # Drop the system streams
        if bolt_service_times.empty:
            raise Exception("Service times for the topology's bolts are unavailable")

        bolt_service_times.drop(["component", "stream"], axis=1, inplace=True)

        return bolt_service_times
