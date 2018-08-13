# Copyright 2018 Twitter, Inc.
# Licensed under the Apache License, Version 2.0
# http://www.apache.org/licenses/LICENSE-2.0

""" This module contains methods for validating the performance predictions of
the Caladrius Heron queueing theory model."""

import os
import sys
import argparse
import logging

import datetime as dt

from typing import List, Tuple, Dict, Any

import requests
import pandas as pd

from caladrius import loader, logs
from caladrius.graph.gremlin.client import GremlinClient
from caladrius.metrics.heron.client import HeronMetricsClient
from caladrius.model.topology.heron.queueing_theory import QTTopologyModel
from caladrius.tests.validation.heron import helpers as heron_helper
from caladrius.tests.validation import helpers as validation_helper
from caladrius.common.heron import zookeeper

LOG: logging.Logger = logging.getLogger(__name__)


def compare(metrics_client: HeronMetricsClient,
            spout_state: Dict[int, Dict[str, float]],
            actual_instance_arrs: pd.DataFrame,
            topology_model: QTTopologyModel,
            topology_id: str, cluster: str, environ: str, start: dt.datetime,
            end: dt.datetime, metric_bucket_length: int, **kwargs):

    # Predict the arrival rates from the spout state during this period
    predicted_instance_arrs, stmgr_arrs = \
        topology_model.predict_arrival_rates(topology_id, cluster, environ,
                                             spout_state, start, end,
                                             metric_bucket_length)
    predicted_instance_arrs = \
        (predicted_instance_arrs.rename(
            index=str, columns={"arrival_rate":
                                "predicted_arrival_rates_tps"}))

    arrs_combined: pd.DataFrame = \
        actual_instance_arrs.merge(predicted_instance_arrs, on="task")

    arrs_combined["error"] = ((arrs_combined["predicted_arrival_rates_tps"] -
                               arrs_combined["actual_arrival_rates_tps"]) /
                              arrs_combined["actual_arrival_rates_tps"])

    return arrs_combined


def run(config: Dict[str, Any], metrics_client: HeronMetricsClient,
        total_hours: int, period_length_secs: int,
        topology_model: QTTopologyModel, topology_id: str, cluster: str,
        environ: str, metric_bucket_length: int, **kwargs: Any):

    start: dt.datetime = (dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc)
                          - dt.timedelta(hours=total_hours))

    zk_config = config["heron.topology.models.config"]
    last_updated: dt.datetime = zookeeper.last_topo_update_ts_html(
        zk_config["heron.statemgr.connection.string"],
        zk_config["heron.statemgr.root.path"], topology_id,
        zk_config["zk.time.offset"]).astimezone(dt.timezone.utc)

    if start < last_updated:
        update_err: str = (f"The provided total hours ({total_hours}) will "
                           f"result in a start time ({start.isoformat()}) "
                           f"which is before the last update to "
                           f"{topology_id}'s physical plan "
                           f"({last_updated.isoformat()})")
        LOG.error(update_err)
        raise RuntimeError(update_err)

    periods: List[Tuple[dt.datetime, dt.datetime]] = \
        validation_helper.create_start_end_list(total_hours,
                                                period_length_secs)

    output: pd.DataFrame = None

    for j, (traffic_start, traffic_end) in enumerate(periods):

        LOG.info("Using metrics sourced from %s to %s",
                 traffic_start.isoformat(), traffic_end.isoformat())

        LOG.info("\n\nComparing prediction, using metrics from period "
                 "%d and traffic from period %d, to actual performance"
                 " during period %d\n", j, j, j)
        try:

            spout_state = heron_helper.get_spout_state(
                metrics_client, topology_id, cluster, environ,
                config["heron.tracker.url"], traffic_start, traffic_end,
                60, "mean")

            # Get the actual arrival rates at all instances
            actual_arrs: pd.DataFrame = \
                metrics_client.get_tuple_arrivals_at_stmgr(topology_id, cluster,
                                                            environ, traffic_start,
                                                            traffic_end, **kwargs)

            actual_arrs: pd.DataFrame = \
                (actual_arrs.groupby(["task", "component", "timestamp"]).sum().reset_index())

            actual_arrs["arrival_rate_tps"] = (actual_arrs["num-tuples"] / 60)

            actual_instanceå_arrs: pd.DataFrame = \
                (actual_arrs.groupby(["component", "task"])
                 ["arrival_rate_tps"].mean().reset_index()
                 .rename(index=str, columns={"arrival_rate_tps":
                                             "actual_arrival_rates_tps"}))

            results: pd.DataFrame = compare(
                metrics_client, spout_state, actual_instance_arrs,
                topology_model, topology_id,
                cluster, environ, traffic_start, traffic_end,
                metric_bucket_length, **kwargs)

        except ConnectionRefusedError as cr_err:
            LOG.error("Connection was refused with message: %s",
                      str(cr_err))
        except ConnectionResetError as cre_err:
            LOG.error("Connection was reset with message: %s",
                      str(cre_err))
        except requests.exceptions.ConnectionError as req_err:
            LOG.error("Connection error with message: %s", str(req_err))
        except Exception as err:
            LOG.error("Error (%s) with message: %s", str(type(err)),
                      str(err))
            raise err
        else:
            results["traffic_start"] = traffic_start
            results["traffic_end"] = traffic_end

            if output is not None:
                output = output.append(results, ignore_index=True)
            else:
                output = results

    return output


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
                        help=("Output directory to save results DataFrames."))

    parser.add_argument("-t", "--topology", required=True)
    parser.add_argument("-c", "--cluster", required=True)
    parser.add_argument("-e", "--environ", required=True)
    parser.add_argument("-hr", "--hours", type=float, required=True)
    parser.add_argument("-p", "--period", type=float, required=True)

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
    qt_model: QTTopologyModel = QTTopologyModel(
        CONFIG["heron.topology.models.config"], metrics_client, graph_client)

    results: pd.DataFrame = run(
        CONFIG, metrics_client, ARGS.hours, ARGS.period, qt_model,
        ARGS.topology, ARGS.cluster, ARGS.environ,
        int(CONFIG["heron.topology.models.config"]["metric.bucket.length"]))

    if ARGS.output_dir:

        if not os.path.exists(ARGS.output_dir):
            os.makedirs(ARGS.output_dir)

        results.to_csv(os.path.join(ARGS.output_dir,
                                    (f"{ARGS.topology}_{ARGS.cluster}_"
                                     f"{ARGS.environ}_arrival_rates.csv")))

    print("\n#### ARRIVAL RATE VALIDATION ####\n")

    print("\nError overall:\n")
    print(results.error.describe().to_string())

    print("\nError per component:\n")
    print(results.groupby("component").error.describe().to_string())

    print("\nError per task:\n")
    print(results.groupby("task").error.describe().to_string())