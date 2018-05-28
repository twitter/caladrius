import os
import sys
import argparse
import logging

import datetime as dt

from typing import List, Tuple, Dict, Any

import pandas as pd

from caladrius import loader, logs
from caladrius.graph.gremlin.client import GremlinClient
from caladrius.metrics.heron.client import HeronMetricsClient
from caladrius.model.topology.heron.queueing_theory import QTTopologyModel
from caladrius.tests.validation.heron import helpers as heron_helper
from caladrius.tests.validation import helpers as validation_helper

LOG: logging.Logger = logging.getLogger(__name__)


def compare(metrics_client: HeronMetricsClient,
            spout_state: Dict[int, Dict[str, float]],
            topology_model: QTTopologyModel,
            topology_id: str, cluster: str, environ: str,  start: dt.datetime,
            end: dt.datetime, metric_bucket_length: int, **kwargs):

    # ## ARRIVAL RATES ##

    # Get the actual arrival rates at all instances
    actual_arrs: pd.DataFrame = \
        metrics_client.get_arrival_rates(topology_id, cluster, environ, start,
                                         end, **kwargs)

    actual_instance_arrs: pd.DataFrame = \
        (actual_arrs.groupby("task")["arrival_rate_tps"].median().reset_index()
         .rename(index=str, columns={"arrival_rate_tps":
                                     "actual_arrival_rates_tps"}))

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

    periods: List[Tuple[dt.datetime, dt.datetime]] = \
        validation_helper.create_start_end_list(total_hours,
                                                period_length_secs)

    output: pd.DataFrame = None

    for i, (start, end) in enumerate(periods):

        LOG.info("Comparing period %d of %d from %s to %s", i, len(periods),
                 start.isoformat(), end.isoformat())

        spout_state = heron_helper.get_spout_state(
            metrics_client, topology_id, cluster, environ,
            config["heron.tracker.url"], start, end, 60, "median")

        results: pd.DataFrame = compare(
            metrics_client, spout_state, topology_model, topology_id, cluster,
            environ, start, end, metric_bucket_length, **kwargs)

        results["period_start"] = start
        results["period_end"] = end

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

    print(results.to_string())

    print(results.error.describe().to_string())
