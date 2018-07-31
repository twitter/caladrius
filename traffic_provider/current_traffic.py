from caladrius.traffic_provider.trafficprovider import TrafficProvider
from caladrius.metrics.heron.client import HeronMetricsClient
from caladrius.model.topology.heron.helpers import convert_arr_rate_to_mean_arr_rate, \
    convert_throughput_to_inter_arr_times

import datetime as dt


class CurrentTraffic(TrafficProvider):
    """ This module takes in the metrics client and uses it
    to provide current traffic information.
    """
    def __init__(self, metrics_client: HeronMetricsClient, topology_id: str,
                 cluster: str, environ: str, start: [dt.datetime], end: [dt.datetime],
                 **other_kwargs) -> None:

        self.metrics_client: HeronMetricsClient = metrics_client
        self.topology_id = topology_id
        self.cluster = cluster
        self.environ = environ
        self.start = start
        self.end = end
        self.kwargs = other_kwargs
        self.tuples = self.metrics_client.get_tuple_arrivals_at_stmgr\
            (self.topology_id, cluster, environ, start, end, **other_kwargs)

    def tuple_arrivals(self):
        return self.tuples

    def arrival_rates(self):
        return convert_arr_rate_to_mean_arr_rate(self.tuples)

    def inter_arrival_times(self):
        return convert_throughput_to_inter_arr_times(self.tuples)
