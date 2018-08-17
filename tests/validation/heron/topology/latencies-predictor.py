# Copyright 2018 Twitter, Inc.
# Licensed under the Apache License, Version 2.0
# http://www.apache.org/licenses/LICENSE-2.0

""" This module contains methods for validating the latency predictions of
the Caladrius Heron queueing theory model."""


import argparse
import datetime as dt
import logging
import os
import sys
from typing import Dict, Any

import pandas as pd
from caladrius import loader, logs
from caladrius.graph.gremlin.client import GremlinClient
from caladrius.graph.utils.heron import graph_check, paths_check
from caladrius.metrics.heron.client import HeronMetricsClient
from caladrius.model.topology.heron.queueing_theory import QTTopologyModel
from caladrius.traffic_provider.current_traffic import CurrentTraffic
from gremlin_python.process.graph_traversal import outE, not_

LOG: logging.Logger = logging.getLogger(__name__)

HISTORICAL_METRICS_DURATION = 120


def _create_parser() -> argparse.ArgumentParser:

    parser: argparse.ArgumentParser = argparse.ArgumentParser(
        description=("This program validates the predictions of the "
                     "Caladrius queuing theory performance modelling system "
                     "for Heron Topologies"))

    parser.add_argument("--config", required=True,
                        help=("Path to the config file with data required by "
                              "all configured models and classes"))
    parser.add_argument("-q", "--quiet", required=False, action="store_true",
                        help=("Optional flag indicating if console log output "
                              "should be suppressed"))
    parser.add_argument("--debug", required=False, action="store_true",
                        help=("Optional flag indicating if debug level "
                              "information should be displayed"))
    parser.add_argument("-od", "--output_dir", required=True,
                        help="Output directory to save results DataFrames.")

    parser.add_argument("-t", "--topology", required=True)
    parser.add_argument("-c", "--cluster", required=True)
    parser.add_argument("-e", "--environ", required=True)

    return parser


if __name__ == "__main__":

    ARGS: argparse.Namespace = _create_parser().parse_args()

    try:
        CONFIG: Dict[str, Any] = loader.load_config(ARGS.config)
    except FileNotFoundError:
        print(f"Config file: {ARGS.config} was not found. Aborting...",
              file=sys.stderr)
        sys.exit(1)
    else:
        if not ARGS.quiet:
            print("\nStarting Caladrius Heron Validation\n")
            print(f"Loading configuration from file: {ARGS.config}")

    CONFIG: Dict[str, Any] = loader.load_config(ARGS.config)
    if not os.path.exists(CONFIG["log.file.dir"]):
        os.makedirs(CONFIG["log.file.dir"])

    LOG_FILE: str = CONFIG["log.file.dir"] + "/validation_heron.log"

    logs.setup(console=(not ARGS.quiet), logfile=LOG_FILE, debug=ARGS.debug)

    # GRAPH CLIENT
    graph_client: GremlinClient = \
        loader.get_class(CONFIG["graph.client"])(CONFIG["graph.client.config"])

    # HERON METRICS CLIENT
    metrics_client: HeronMetricsClient = \
        loader.get_class(CONFIG["heron.metrics.client"])(
            CONFIG["heron.metrics.client.config"])

    # TOPOLOGY PERFORMANCE MODEL

    cluster = ARGS.cluster
    environ = ARGS.environ
    topology = ARGS.topology

    topology_latencies: pd.DataFrame = pd.DataFrame(columns=['topology', 'av_actual_latency', 'std_actual_latency',
                                                             'av_calculated_latency', 'std_predicted_latency'])
    system_metrics: pd.DataFrame = pd.DataFrame(columns=['topology', 'component', 'av_gc', 'std_gc',
                                                         'av_cpu_load', 'std_cpu_load'])

    # Make sure we have a current graph representing the physical plan for
    # the topology
    graph_check(graph_client, CONFIG["heron.topology.models.config"], CONFIG["heron.tracker.url"],
                cluster, environ, topology)

    # Make sure we have a file containing all paths for the job
    paths_check(graph_client, CONFIG["heron.topology.models.config"], cluster, environ, topology)
    model_kwargs = dict()

    model_kwargs["zk.time.offset"] = CONFIG["heron.topology.models.config"]["zk.time.offset"]
    model_kwargs["heron.statemgr.root.path"] = CONFIG["heron.topology.models.config"]["heron.statemgr.root.path"]
    model_kwargs["heron.statemgr.connection.string"] = \
        CONFIG["heron.topology.models.config"]["heron.statemgr.connection.string"]

    now = dt.datetime.now()
    start, end = now - dt.timedelta(minutes=HISTORICAL_METRICS_DURATION), now

    traffic_provider: CurrentTraffic = CurrentTraffic(metrics_client, graph_client, topology, cluster,
                                                      environ, start, end, {}, **model_kwargs)
    qt: QTTopologyModel = QTTopologyModel(CONFIG["heron.topology.models.config"], metrics_client, graph_client)
    results = pd.DataFrame(qt.find_current_instance_waiting_times(topology_id=topology, cluster=cluster, environ=environ,
                                                           traffic_source=traffic_provider, start=start, end=end,
                                                           **model_kwargs))

    sinks = graph_client.graph_traversal.V().has("topology_id", topology).hasLabel("bolt").\
        where(not_(outE("logically_connected"))).properties('component').value().dedup().toList()

    actual_latencies: pd.DataFrame = pd.DataFrame

    for sink in sinks:
        result = metrics_client.get_end_to_end_latency(topology, cluster, environ, sink, start, end)
        result["average_latency"] = result["end_to_end_latency"] / result["tuple_count"]
        if actual_latencies.empty:
            actual_latencies = result
        else:
            actual_latencies.append(result, ignore_index=True)

    topology_latencies = topology_latencies.append({'topology': topology,
                                                    'av_actual_latency': actual_latencies['average_latency'].mean(),
                                                    'std_actual_latency': actual_latencies['average_latency'].std(),
                                                    'av_calculated_latency': results['latency'].mean(),
                                                    'std_predicted_latency': results['latency'].std()},
                                                   ignore_index=True)
    CPU_LOAD = metrics_client.get_cpu_load(topology, cluster, environ, start, end)
    GC_TIME = metrics_client.get_gc_time(topology, cluster, environ, start, end)
    CAPACITY = metrics_client.get_capacity(topology, cluster, environ, start, end)

    load = pd.DataFrame(columns=['topology', 'component', 'av_cpu_load', 'std_cpu_load'])
    gc = pd.DataFrame(columns = ['topology', 'component', 'av_gc', 'std_gc'])
    capacity = pd.DataFrame(columns=['topology', 'component', 'av_capacity', 'std_capacity'])
    for component, data in CPU_LOAD.groupby(["component"]):
        load = load.append({"topology": topology, "component": component,
                            "av_cpu_load": data["cpu-load"].mean(), "std_cpu_load": data["cpu-load"].std()},
                           ignore_index=True)

    for component, data in GC_TIME.groupby(["component"]):
        gc = gc.append({"topology": topology,  "component": component,
                        "av_gc": data["gc-time"].mean(), "std_gc": data["gc-time"].std()},
                       ignore_index=True)

    for component, data in CAPACITY.groupby(["component"]):
        capacity = capacity.append({"topology": topology,  "component": component,
                        "av_capacity": data["capacity"].mean(), "std_capacity": data["capacity"].std()},
                       ignore_index=True)
    merged = load.merge(gc, on=["component", "topology"])
    merged = merged.merge(capacity, on=["component", "topology"])
    system_metrics = system_metrics.append(merged, ignore_index=True, sort=True)

    system_metrics.to_csv(ARGS.output_dir, f"{ARGS.topology}_{ARGS.cluster}_{ARGS.environ}_system_metrics.csv")
    topology_latencies.to_csv(ARGS.output_dir, f"{ARGS.topology}_{ARGS.cluster}_{ARGS.environ}_latencies.csv")
