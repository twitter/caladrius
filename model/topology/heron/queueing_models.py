# Copyright 2018 Twitter, Inc.
# Licensed under the Apache License, Version 2.0
# http://www.apache.org/licenses/LICENSE-2.0

""" This module models different queues and performs relevant calculations for it."""

import datetime as dt
import pandas as pd

from caladrius.metrics.client import MetricsClient
from caladrius.graph.gremlin.client import GremlinClient
from caladrius.model.topology.heron.abs_queueing_models import QueueingModels
from caladrius.model.topology.heron.helpers import *
from caladrius.graph.utils.heron import get_all_paths

LOG: logging.Logger = logging.getLogger(__name__)


def littles_law(merged: pd.DataFrame) -> pd.DataFrame:
    """
           This function applies Little's Law to find out the expected length of the queue
           (https://en.wikipedia.org/wiki/Little%27s_law)
           Arguments:
                   merged (pd.DataFrame) : This dataframe is expected to contain task number,
                   arrival rate, and tuple waiting time.
    """
    merged["queue-size"] = merged["mean_waiting_time"] * merged["mean_arrival_rate"]
    return merged


class MMCQueue(QueueingModels):
    """
    This class in effect models an MM1 Queue. In queueing theory, an M/M/1 queue represents
    the queue length in a system having a single server where arrival times of new jobs can
    be described using a Poisson process and job service times can be described using an exponential
    distribution. An extension of this model is one with multiple servers (denoted by variable 'c')
    and is called an M/M/c queue.
    """
    def __init__(self, metrics_client: MetricsClient, graph_client: GremlinClient, topology_id: str,
                 cluster: str, environ: str, start: dt.datetime, end: dt.datetime, other_kwargs: dict):
        """
        This function initializes relevant variables to calculate queue related metrics
        given an M/M/c model.
        """

        super().__init__(metrics_client, graph_client, topology_id, cluster, environ, start, end, other_kwargs)

        # find all end-to-end paths in topology
        self.paths = get_all_paths(self.graph_client, topology_id)

        # Get the service time for all elements
        service_times: pd.DataFrame = self.metrics_client.get_service_times(
            topology_id, cluster, environ, start, end, **other_kwargs)

        # Drop the system streams
        self.service_times = (service_times[~service_times["stream"].str.contains("__")])

        # Get arrival rates per ms for all instances
        # We should not be using this any more.
        arrival_rate: pd.DataFrame = self.metrics_client.get_tuple_arrivals_at_stmgr(
            topology_id, cluster, environ, start, end, **other_kwargs)

        # Finding mean waiting time and validating queue size
        self.service_rate = convert_service_times_to_rates(service_times)
        self.arrival_rate = convert_arr_rate_to_mean_arr_rate(arrival_rate)

    def average_waiting_time(self) -> pd.DataFrame:
        merged: pd.DataFrame = self.service_rate.merge(self.arrival_rate, on=["task"])
        merged["mean_waiting_time"] = merged["mean_arrival_rate"] / \
            (merged["mean_service_rate"] * (merged["mean_service_rate"] - merged["mean_arrival_rate"]))
        return merged

    def average_queue_size(self) -> pd.DataFrame:
        merged: pd.DataFrame = self.service_rate.merge(self.arrival_rate, on=["task"])
        merged["utilization"] = merged["mean_arrival_rate"]/merged["mean_service_rate"]
        merged["queue-size"] = (merged["utilization"] ** 2)/(1 - merged["utilization"])

        return merged

    def end_to_end_latencies(self) -> list:
        merged: pd.DataFrame = self.average_waiting_time()
        queue_size: pd.DataFrame = self.average_queue_size()
        merged = merged.merge(queue_size, on=["task"])[["utilization", "task", "mean_waiting_time", "queue-size", "mean_arrival_rate_x"]]
        merged = merged.rename(columns={'mean_arrival_rate_x': 'mean_arrival_rate'})
        LOG.info(merged)
        return find_end_to_end_latencies(self.paths, merged, self.service_times)


class GGCQueue(QueueingModels):
    """
    This class in effect models an GG1 Queue. The G/G/1 queue represents the queue length
    in a system with a single server where interarrival times have a general (or arbitrary)
    distribution and service times have a (different) general distribution. This system can fit
    more realistic scenarios as arrival rates and processing rates do not necessarily fit probabilistic
    distributions (such as the Poisson distribution, used to describe arrival rates in M/M/1 queues).
    """
    def __init__(self, metrics_client: MetricsClient, graph_client: GremlinClient, topology_id: str,
                 cluster: str, environ: str, start: dt.datetime, end: dt.datetime, other_kwargs: dict):
        """
        This function initializes relevant variables to calculate queue related metrics
        given a G/G/c model
        As both data arrival distributions and processing distributions are general,
        it is difficult to find an exact value for the waiting time. However, we can find
        a probable upperbound.
        There is a great deal of detail available here
        http://home.iitk.ac.in/~skb/qbook/Slide_Set_12.PDF and
        http://www.math.nsc.ru/LBRT/v1/foss/gg1_2803.pdf
        """
        super().__init__(metrics_client, graph_client, topology_id, cluster, environ, start, end, other_kwargs)

        # find all end-to-end paths in topology
        self.paths = get_all_paths(self.graph_client, topology_id)

        # Remove the start and end time kwargs so we don't supply them twice to
        # the metrics client.

        # Get the service time for all elements
        service_times: pd.DataFrame = self.metrics_client.get_service_times(
            topology_id, cluster, environ, start, end, **other_kwargs)

        # Drop the system streams
        service_times = (service_times[~service_times["stream"].str.contains("__")])

        # Get execute counts for all elements for validation
        execute_counts: pd.DataFrame = self.metrics_client.get_execute_counts(
            topology_id, cluster, environ, start, end, **other_kwargs)

        # Drop the system streams
        self.service_times = (service_times[~service_times["stream"].str.contains("__")])

        # Get number of arrivals at stream managers
        tuple_arrivals: pd.DataFrame = self.metrics_client.get_tuple_arrivals_at_stmgr\
            (topology_id, cluster, environ, start, end, **other_kwargs)

        self.tuple_arrivals: pd.DataFrame = tuple_arrivals
        self.execute_counts: pd.DataFrame = execute_counts
        self.inter_arrival_time_stats: pd.DataFrame = convert_throughput_to_inter_arr_times(self.tuple_arrivals)
        self.service_stats: pd.DataFrame = process_execute_latencies(self.service_times)
        self.arrival_rate = convert_arr_rate_to_mean_arr_rate(self.tuple_arrivals)
        self.service_rate = convert_service_times_to_rates(self.service_times)
        self.queue_size = pd.DataFrame

    def average_waiting_time(self) -> pd.DataFrame:
        merged: pd.DataFrame = self.service_stats.merge(self.inter_arrival_time_stats, on=["task"])

        merged["utilization"] = merged["mean_service_time"] / merged["mean_inter_arrival_time"]
        merged["coeff_var_arrival"] = merged["std_inter_arrival_time"] / merged["mean_inter_arrival_time"]
        merged["coeff_var_service"] = merged["std_service_time"] / merged["mean_service_time"]
        merged["mean_waiting_time"] = merged["utilization"] / (1 - merged["utilization"]) * \
                                           merged["mean_service_time"] * ((merged["coeff_var_service"] ** 2
                                                                           + merged["coeff_var_arrival"] ** 2) / 2)
        return merged

    def average_queue_size(self) -> pd.DataFrame:
        # complete by calling little's law

        merged: pd.DataFrame = self.arrival_rate.merge(self.service_rate, on=["task"])
        average_waiting_time = self.average_waiting_time()
        average_waiting_time = average_waiting_time[["task", "mean_waiting_time"]]
        merged = merged.merge(average_waiting_time, on=["task"])
        self.queue_size: pd.DataFrame = littles_law(merged)

        self.queue_size["scaled-queue-size"] = self.queue_size["queue-size"] * 60 * 1000 # because it was calculated per ms
        LOG.info(self.queue_size[["task", "mean_waiting_time", "mean_arrival_rate", "scaled-queue-size", "queue-size"]])
        validate_queue_size(self.execute_counts, self.tuple_arrivals)

        return self.queue_size

    def end_to_end_latencies(self) -> list:
        merged: pd.DataFrame = self.average_waiting_time()
        # for validation only
        queue_size: pd.DataFrame = self.average_queue_size()

        subset: pd.DataFrame = queue_size[["task", "queue-size"]]
        merged = merged.merge(subset, on=["task"])[["utilization", "task", "mean_waiting_time", "queue-size"]]

        return find_end_to_end_latencies(self.paths, merged, self.service_times)
