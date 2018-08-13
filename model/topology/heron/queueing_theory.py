# Copyright 2018 Twitter, Inc.
# Licensed under the Apache License, Version 2.0
# http://www.apache.org/licenses/LICENSE-2.0

""" This module contains classes and methods for modelling the performance of
Heron topologies using queueing theory. """

import logging

import datetime as dt
import pandas as pd
from typing import Any, cast, Dict, Tuple


from caladrius.model.topology.heron.base import HeronTopologyModel
from caladrius.model.topology.heron.abs_queueing_models import QueueingModels
from caladrius.model.topology.heron.queueing_models import MMCQueue, GGCQueue
from caladrius.metrics.heron.client import HeronMetricsClient
from caladrius.graph.gremlin.client import GremlinClient
from caladrius.graph.analysis.heron import arrival_rates
from caladrius.graph.utils.heron import graph_check, read_paths
from caladrius.performance_prediction.predictor import Predictor
from caladrius.performance_prediction.simple_predictor import SimplePredictor
from caladrius.traffic_provider.trafficprovider import TrafficProvider

LOG: logging.Logger = logging.getLogger(__name__)


class QTTopologyModel(HeronTopologyModel):
    """ This model implementation predict topology performance using queueing
    theory.

    NOTE: This is very much a work in progress and so far only does comparisons
    of arrival rates and service times to check QT model validity.
    """

    name: str = "queueing_theory"

    description: str = ("Models the topology as a queuing network and flags "
                        "if back pressure is likely at instances.")

    def __init__(self, config: Dict[str, Any],
                 metrics_client: HeronMetricsClient,
                 graph_client: GremlinClient) -> None:
        super().__init__(config, metrics_client, graph_client)

        self.metrics_client: HeronMetricsClient
        self.tracker_url: str = config["heron.tracker.url"]

    def predict_arrival_rates(self, topology_id: str,
                              cluster: str, environ: str,
                              spout_traffic: Dict[int, Dict[str, float]],
                              start: dt.datetime, end: dt.datetime,
                              metric_bucket_length: int,
                              topology_ref: str = None, **kwargs: Any
                              ) -> Tuple[pd.DataFrame, pd.DataFrame]:

        if not topology_ref:
            # Get the reference of the latest physical graph entry for this
            # topology, or create a physical graph if there are non.
            topology_ref = graph_check(self.graph_client, self.config,
                                       self.tracker_url, cluster, environ,
                                       topology_id)

        # Predict Arrival Rates for all elements
        instance_ars: pd.DataFrame
        strmgr_ars: pd.DataFrame
        instance_ars, strmgr_ars =  \
            arrival_rates.calculate(
                self.graph_client, self.metrics_client, topology_id, cluster,
                environ, topology_ref, start, end, metric_bucket_length,
                self.tracker_url, spout_traffic, **kwargs)

        # Sum the arrivals from each source component of each incoming stream
        instance_ars.groupby(["task", "incoming_stream"]).sum()

        in_ars: pd.DataFrame =  \
            (instance_ars.groupby(["task", "incoming_stream"]).sum()
             .reset_index().rename(index=str,
                                   columns={"incoming_stream": "stream"}))

        return in_ars, strmgr_ars

    def find_current_instance_waiting_times(self, topology_id: str, cluster: str,
                                            environ: str, traffic_source: TrafficProvider,
                                            start: dt.datetime, end: dt.datetime,
                                            **kwargs: Any) -> list:

        LOG.info("Calculating end to end performance latency of topology "
                 "%s using queueing theory", topology_id)

        # Remove the start and end time kwargs so we don't supply them twice to
        # the metrics client.
        other_kwargs: Dict[str, Any] = {key: value
                                        for key, value in kwargs.items()
                                        if key not in ["start", "end"]}

        paths = read_paths(other_kwargs, topology_id, cluster, environ)

        queue: QueueingModels = GGCQueue(self.graph_client, self.metrics_client, paths,
                                         topology_id, cluster, environ, start, end, traffic_source, other_kwargs)
        return queue.end_to_end_latencies()

    def predict_current_performance(
            self, topology_id: str, cluster: str, environ: str,
            spout_traffic: Dict[int, Dict[str, float]],
            **kwargs: Any) -> pd.DataFrame:
        """
        Arguments:
            topology_id (str): The topology identification string
            spout_traffic (dict):   The expected output of the spout instances.
                                    These emit values should be in tuples per
                                    second (tps) otherwise they will not match
                                    with the service time measurements.
        """
        # TODO: check spout traffic keys are integers!
        start, end = get_start_end_times(**kwargs)

        metric_bucket_length: int = cast(int,
                                         self.config["metric.bucket.length"])

        LOG.info("Predicting traffic levels and backpressure of currently running "
                 "topology %s using queueing theory model", topology_id)

        # Remove the start and end time kwargs so we don't supply them twice to
        # the metrics client.
        # TODO: We need to make this cleaner? Add start and end to topo model?
        other_kwargs: Dict[str, Any] = {key: value
                                        for key, value in kwargs.items()
                                        if key not in ["start", "end"]}

        # Get the service time for all elements
        service_times: pd.DataFrame = self.metrics_client.get_service_times(
            topology_id, cluster, environ, start, end, **other_kwargs)
        if service_times.empty:
            raise Exception("Metric client returned empty data frame for service times.")

        # Calculate the service rate for each instance
        service_times["tuples_per_sec"] = 1.0 / (service_times["latency_ms"] /
                                                 1000.0)

        # Drop the system streams
        service_times = (service_times[~service_times["stream"]
                         .str.contains("__")])

        # Calculate the median service time and rate
        service_time_summary: pd.DataFrame = \
            (service_times[["task", "stream", "latency_ms", "tuples_per_sec"]]
             .groupby(["task", "stream"]).median().reset_index())

        # Get the reference of the latest physical graph entry for this
        # topology, or create a physical graph if there are non.
        topology_ref: str = graph_check(self.graph_client, self.config,
                                        self.tracker_url, cluster, environ,
                                        topology_id)

        # Predict the arrival rate at all instances with the supplied spout
        # traffic
        in_ars, strmgr_ars = self.predict_arrival_rates(
            topology_id, cluster, environ, spout_traffic, start, end,
            metric_bucket_length, topology_ref)

        combined: pd.DataFrame = service_time_summary.merge(
            in_ars, on=["task", "stream"])

        combined["capacity"] = (combined["arrival_rate"] /
                                combined["tuples_per_sec"]) * 100.0

        combined["back_pressure"] = combined["capacity"] > 100.0

        return combined

    def predict_packing_plan(self, topology_id: str, cluster: str, environ: str, start: dt.datetime,
                             end: dt.datetime, traffic_provider: TrafficProvider, **kwargs: Any) -> Dict[str, Any]:

        LOG.info("Calculating a new packing plan of the topology %s, based on performance from %s to %s",
                 topology_id, str(start), str(end))
        # Remove the start and end time kwargs so we don't supply them twice to
        # the metrics client.
        other_kwargs: Dict[str, Any] = {key: value
                                        for key, value in kwargs.items()
                                        if key not in ["start", "end"]}

        paths = read_paths(other_kwargs, topology_id, cluster, environ)

        queue: QueueingModels = GGCQueue(self.graph_client, self.metrics_client, paths,
                                         topology_id, cluster, environ,
                                         start, end, traffic_provider, other_kwargs)
        p: Predictor = SimplePredictor(topology_id, cluster, environ, start,
                                       end, self.tracker_url, self.metrics_client, self.graph_client,
                                       queue, **other_kwargs)

        return p.create_new_plan()


def get_start_end_times(**kwargs) -> (dt.datetime, dt.datetime):
    if "start" in kwargs and "end" in kwargs:
        start_ts: int = int(kwargs["start"])
        start: dt.datetime = dt.datetime.utcfromtimestamp(start_ts)
        end_ts: int = int(kwargs["end"])
        end: dt.datetime = dt.datetime.utcfromtimestamp(end_ts)
        LOG.info("Start and end time stamps supplied, using metric "
                 "gathering period from %s to %s", start.isoformat(),
                 end.isoformat())
    elif "start" in kwargs and "end" not in kwargs:
        end = dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc)
        start_ts = int(kwargs["start"])
        start = dt.datetime.utcfromtimestamp(start_ts)
        LOG.info("Only start time (%s) was supplied. Setting end time to "
                 "UTC now: %s", start.isoformat(), end.isoformat())
    elif "source_hours" in kwargs:
        end = dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc)
        start = end - dt.timedelta(hours=int(kwargs["source_hours"]))
        LOG.info("Source hours provided, using metric gathering period "
                 "from %s to %s", start.isoformat(), end.isoformat())
    elif "source_mins" in kwargs:
        end = dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc)
        start = end - dt.timedelta(minutes=int(kwargs["source_mins"]))
        LOG.info("Source mins provided, using metric gathering period "
                 "from %s to %s", start.isoformat(), end.isoformat())

    else:
        err_msg: str = ("Neither 'start', 'end' or 'source_hours' or 'source_mins' "
                        "key word arguments were supplied. Either 'start',"
                        " 'start' and 'end' or 'source_hours' should be "
                        "provided")
        LOG.error(err_msg)
        raise RuntimeError(err_msg)

    return start, end
