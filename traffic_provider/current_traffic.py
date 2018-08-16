from caladrius.traffic_provider.trafficprovider import TrafficProvider
from caladrius.metrics.heron.client import HeronMetricsClient
from caladrius.model.topology.heron.helpers import convert_arr_rate_to_mean_arr_rate, \
    convert_throughput_to_inter_arr_times
from caladrius.graph.gremlin.client import GremlinClient

import datetime as dt
from gremlin_python.process.graph_traversal import outE
import pandas as pd
from typing import Dict, Any


class CurrentTraffic(TrafficProvider):
    """ This module takes in the metrics client and uses it to provide current traffic information.
     As opposed to the predicted traffic provider, it also models the spout information"""
    # we don't need the traffic config but we can add it to make the arguments the same in both traffic providers
    def __init__(self, metrics_client: HeronMetricsClient, graph_client: GremlinClient, topology_id: str,
                 cluster: str, environ: str, start: [dt.datetime], end: [dt.datetime],
                 traffic_config: Dict[str, Any], **other_kwargs) -> None:
        self.graph_client = graph_client
        self.metrics_client: HeronMetricsClient = metrics_client
        self.topology = topology_id
        self.cluster = cluster
        self.environ = environ
        self.start = start
        self.end = end
        self.kwargs = other_kwargs
        self.tuples = self.metrics_client.get_tuple_arrivals_at_stmgr\
            (self.topology, cluster, environ, start, end, **other_kwargs)

        spouts = graph_client.graph_traversal.V().has("topology_id", self.topology). \
            hasLabel("spout").where(outE("logically_connected")).properties('component').value().dedup().toList()

        spout_queue_processing_rate = metrics_client.get_outgoing_queue_processing_rate(
            topology_id, cluster, environ, start, end)
        self.spout_queue_processing_rate = \
            spout_queue_processing_rate.loc[spout_queue_processing_rate['component'].isin(spouts)]

        num_tuples_added_to_spout_gateway_queue = metrics_client.get_out_going_queue_arrival_rate(
            self.topology, cluster, environ, start, end)
        self.num_tuples_added_to_spout_gateway_queue = \
            num_tuples_added_to_spout_gateway_queue.loc[
                num_tuples_added_to_spout_gateway_queue['component'].isin(spouts)]

        spout_tuple_set_size = metrics_client.get_average_tuple_set_size_added_to_outgoing_queue(
            self.topology, cluster, environ, start, end)
        self.spout_tuple_set_size = spout_tuple_set_size.loc[spout_tuple_set_size['component'].isin(spouts)]

        spout_arrival_rates = self.num_tuples_added_to_spout_gateway_queue.\
            rename(index=str, columns={"tuples-added-to-queue": "num-tuples"})
        self.spout_arrival_rates = spout_arrival_rates.\
            merge(self.spout_tuple_set_size, on=["task", "component", "container", "timestamp"])
        self.spout_arrival_rates["num-tuples"] = self.spout_arrival_rates["num-tuples"] *\
                                                 self.spout_arrival_rates["tuple-set-size"]

    def tuple_arrivals(self):
        return self.tuples

    def arrival_rates(self):
        bolt_arrival_rates = convert_arr_rate_to_mean_arr_rate(self.tuples)
        spout_arrival_rates = convert_arr_rate_to_mean_arr_rate(self.spout_arrival_rates)
        arr_rates = spout_arrival_rates.append(bolt_arrival_rates, sort=True)
        return arr_rates

    def inter_arrival_times(self):
        bolt_inter_arrival_times = convert_throughput_to_inter_arr_times(self.tuples)
        spout_arrival_rates = convert_throughput_to_inter_arr_times(self.spout_arrival_rates)
        return bolt_inter_arrival_times.append(spout_arrival_rates, sort=True)

    def service_times(self):
        """
        This function finds out the service times for a topology's spouts and merges them with the service times of bolts.

        Spouts don't actually process tuples -- however, they do have a queue that stores outgoing tuples.
        Here, we meant the amount of time it takes per tuple to flush out tuples in the queue. Times are
        returned in ms
        :return: a dataframe of processing latencies
        """
        merged = self.spout_queue_processing_rate. \
            merge(self.num_tuples_added_to_spout_gateway_queue, on=["timestamp", "component", "task", "container"]). \
            merge(self.spout_tuple_set_size, on=["timestamp", "component", "task", "container"])

        df: pd.DataFrame = pd.DataFrame(columns=['task', 'latency_ms', "timestamp", "component", "container"])
        for index, data in merged.iterrows():
            # tuples processed in a minute
            processed_tuples = data["instance-processing-rate"] * data["tuple-set-size"]
            if (processed_tuples > 0):
            # these are the number of tuples processed per millisecond
                latency = (60 * 1000) / processed_tuples
                df = df.append({'task': data["task"], 'latency_ms': latency,
                            'component': data["component"], "container": data["container"],
                            "timestamp": data["timestamp"]}, ignore_index=True)

        bolt_service_times = self.metrics_client.get_service_times(self.topology, self.cluster,
                                                                   self.environ, self.start, self.end, **self.kwargs)
        if bolt_service_times.empty:
            raise Exception("Service times for the topology's bolts are unavailable")

        bolt_service_times.drop(["stream"], axis=1, inplace=True)

        return df.append(bolt_service_times, sort=True)
