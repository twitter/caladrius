# Copyright 2018 Twitter, Inc.
# Licensed under the Apache License, Version 2.0
# http://www.apache.org/licenses/LICENSE-2.0

""" This module contains methods for validating the performance predictions of
the Caladrius Heron prophet traffic forecasting model."""

import os
import sys
import argparse
import logging
import math

import datetime as dt

from typing import Dict, Any, Tuple, List, Union

import pandas as pd

from caladrius import loader, logs
from caladrius.common.heron import tracker, zookeeper
from caladrius.metrics.heron.client import HeronMetricsClient
from caladrius.model.traffic.heron import prophet

LOG: logging.Logger = \
    logging.getLogger("caladrius.tests.validation.heron.traffic.prophet")


def validate_component_level_stats(metrics_client: HeronMetricsClient, tracker_url: str,
                                  topology_id: str, cluster: str, environ: str,
                                  source_hours_list: List[float],
                                  future_mins_list: List[int], metrics_start: dt.datetime,
                                  metrics_end: dt.datetime
                                  ) -> pd.DataFrame:

    LOG.info("Validating traffic predictions for topology %s for "
             "component level metrics", topology_id)

    # Fetch emit counts that cover the whole possible validation time period
    spout_emit_counts: pd.DataFrame = prophet.get_spout_emissions(
        metrics_client, tracker_url, topology_id, cluster, environ,
        metrics_start, metrics_end)

    source_end: dt.datetime = (metrics_end -
                               dt.timedelta(minutes=max(future_mins_list))).replace(tzinfo=None)

    output: List[Dict[str, Union[str, int, float]]] = []

    for source_duration in source_hours_list:

        model_start: dt.datetime = (source_end -
                                    dt.timedelta(hours=source_duration)).replace(tzinfo=None)

        LOG.info("Predicting future traffic using %f hours of source data from"
                 " %s to %s", source_duration, model_start, source_end)

        model_emits: pd.DataFrame = \
            spout_emit_counts[(spout_emit_counts.timestamp >= model_start) &
                              (spout_emit_counts.timestamp <= source_end)]

        try:
            # Build component models for each instance using source data of the
            # current duration
            component_models: prophet.COMPONENT_MODELS = prophet.build_component_models(
                metrics_client, tracker_url, topology_id, cluster, environ, spout_emits=model_emits)
        except Exception as err:
            LOG.error("Error creating models for topology %s, source duration "
                      "%f: %s", topology_id, source_duration, str(err))
            continue

        for future_duration in future_mins_list:
            # Now use the models created from the current source duration and
            # predict performance for the current future duration
            LOG.info("Predicting traffic %d minutes into the future",
                     future_duration)

            try:
                # the prediction is the mean input rate per instance in the component
                prediction: pd.DataFrame = prophet.run_per_component(
                    component_models, future_duration).rename(
                        index=str, columns={"ds": "timestamp"})
            except Exception as err:
                LOG.error("Error forecasting %d mins ahead for topology %s "
                          "using source duration %f: %s", future_duration,
                          topology_id, source_duration, str(err))
                continue

            validation_end: dt.datetime = \
                source_end + dt.timedelta(minutes=future_duration)

            actual: pd.DataFrame = spout_emit_counts[
                (spout_emit_counts.timestamp >= source_end) &
                (spout_emit_counts.timestamp <= validation_end)]

            combined = actual.merge(prediction, on=["timestamp", "component", "stream"])
            # To avoid inf values we have to remove all rows where emit count
            # is zero
            combined = combined[combined.emit_count > 0]

            combined["residual"] = combined["yhat"] - combined["emit_count"]
            combined["sq_residual"] = (combined["residual"] *
                                       combined["residual"])

            combined["error"] = combined["residual"] / combined["emit_count"]
            combined["sq_error"] = (combined["error"] * combined["error"])

            # We would like to find out the accuracy of the prediction per instance, not just per component
            for (comp, task, stream), data in \
                    combined.groupby(["component", "task", "stream"]):

                rms_residual: float = math.sqrt(data["sq_residual"].mean())
                rms_error: float = math.sqrt(data["sq_error"].mean())

                row: Dict[str, Union[str, int, float]] = {
                    "component": comp, "task": task, "stream": stream,
                    "source_hours": source_duration,
                    "future_mins": future_duration,
                    "rms_residual": rms_residual,
                    "rms_error": rms_error}
                output.append(row)
    return pd.DataFrame(output)


def validate_instance_level_stats(metrics_client: HeronMetricsClient, tracker_url: str,
                                  topology_id: str, cluster: str, environ: str,
                                  source_hours_list: List[float],
                                  future_mins_list: List[int], metrics_start: dt.datetime,
                                  metrics_end: dt.datetime
                                  ) -> pd.DataFrame:

    LOG.info("Validating traffic predictions for topology %s for instance level metrics", topology_id)

    # Fetch emit counts that cover the whole possible validation time period
    spout_emit_counts: pd.DataFrame = prophet.get_spout_emissions(
        metrics_client, tracker_url, topology_id, cluster, environ,
        metrics_start, metrics_end)

    source_end: dt.datetime = (metrics_end -
                               dt.timedelta(minutes=max(future_mins_list))).replace(tzinfo=None)

    output: List[Dict[str, Union[str, int, float]]] = []

    for source_duration in source_hours_list:

        model_start: dt.datetime = (source_end -
                                    dt.timedelta(hours=source_duration)).replace(tzinfo=None)

        LOG.info("Predicting future traffic using %f hours of source data from"
                 " %s to %s", source_duration, model_start, source_end)

        model_emits: pd.DataFrame = \
            spout_emit_counts[(spout_emit_counts.timestamp >= model_start) &
                              (spout_emit_counts.timestamp <= source_end)]

        try:
            # Build instance models for each instance using source data of the
            # current duration
            instance_models: prophet.INSTANCE_MODELS = \
                prophet.build_instance_models(
                    metrics_client, tracker_url, topology_id, cluster, environ,
                    spout_emits=model_emits)
        except Exception as err:
            LOG.error("Error creating models for topology %s, source duration "
                      "%f: %s", topology_id, source_duration, str(err))
            continue

        for future_duration in future_mins_list:
            # Now use the models created from the current source duration and
            # predict performance for the current future duration

            LOG.info("Predicting traffic %d minutes into the future",
                     future_duration)

            try:
                prediction: pd.DataFrame = prophet.run_per_instance_models(
                    instance_models, future_duration).rename(
                        index=str, columns={"ds": "timestamp"})
            except Exception as err:
                LOG.error("Error forecasting %d mins ahead for topology %s "
                          "using source duration %f: %s", future_duration,
                          topology_id, source_duration, str(err))
                continue

            validation_end: dt.datetime = \
                source_end + dt.timedelta(minutes=future_duration)

            actual: pd.DataFrame = spout_emit_counts[
                (spout_emit_counts.timestamp >= source_end) &
                (spout_emit_counts.timestamp <= validation_end)]

            combined = actual.merge(prediction, on=["timestamp", "component",
                                                    "task", "stream"])

            # To avoid inf values we have to remove all rows where emit count
            # is zero
            combined = combined[combined.emit_count > 0]

            combined["residual"] = combined["yhat"] - combined["emit_count"]
            combined["sq_residual"] = (combined["residual"] *
                                       combined["residual"])

            combined["error"] = combined["residual"] / combined["emit_count"]
            combined["sq_error"] = (combined["error"] * combined["error"])

            for (comp, task, stream), data in \
                    combined.groupby(["component", "task", "stream"]):

                rms_residual: float = math.sqrt(data["sq_residual"].mean())
                rms_error: float = math.sqrt(data["sq_error"].mean())

                row: Dict[str, Union[str, int, float]] = {
                    "component": comp, "task": task, "stream": stream,
                    "source_hours": source_duration,
                    "future_mins": future_duration,
                    "rms_residual": rms_residual,
                    "rms_error": rms_error}
                output.append(row)

    return pd.DataFrame(output)


def calc_source_metrics_period(source_hours_list: List[float],
                               future_mins_list: List[int]
                               ) -> Tuple[dt.datetime, dt.datetime]:

    metrics_end: dt.datetime = (dt.datetime.utcnow()
                                .replace(tzinfo=dt.timezone.utc))

    metrics_start: dt.datetime = \
        (metrics_end - dt.timedelta(hours=((max(source_hours_list)) +
                                           (max(future_mins_list)/60))))

    return metrics_start, metrics_end


def run(config: Dict[str, Any], metrics_client: HeronMetricsClient,
        source_hours_list: List[float], future_mins_list: List[int],
        output_dir: str, topology_ids: List[str] = None, comp: bool = False) -> pd.DataFrame:

    metrics_start, metrics_end = calc_source_metrics_period(source_hours_list,
                                                            future_mins_list)

    tracker_url: str = config["heron.tracker.url"]

    topologies: pd.DataFrame = tracker.get_topologies(tracker_url)

    output: pd.DataFrame = None

    if not topology_ids:
        LOG.info("Validating all topologies registered with the Heron Tracker")
        topology_rows = topologies.itertuples()
        total_topos: int = int(topologies.topology.count())
    else:
        LOG.info("Validating topologies: %s", str(topology_ids))
        topos = \
            topologies[topologies["topology"].isin(topology_ids)]
        total_topos = int(topos.topology.count())
        topology_rows = topos.itertuples()

    for i, row in enumerate(topology_rows):

        LOG.info("Processing topology %d of %d", i+1, total_topos)

        try:
            # Check we can validate this topology against the times provided
            last_updated: dt.datetime = zookeeper.last_topo_update_ts_html(
                config["heron.statemgr.connection.string"],
                config["heron.statemgr.root.path"], row.topology,
                config["zk.time.offset"])
        except Exception as zk_err:
            LOG.error("Error fetching last update timestamp from zookeeper for"
                      "topology %s: %s", row.topology, str(zk_err))
            continue

        if last_updated.astimezone(dt.timezone.utc) > metrics_start:

            hours_ago: float = \
                ((dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc) -
                 last_updated.astimezone(dt.timezone.utc)).total_seconds() /
                 3600)

            error_source_hours: List[float] = [hour for hour in
                                               source_hours_list
                                               if hour > hours_ago]

            LOG.info("Topology updated %f hours ago too recently to fully "
                     "validate against source hours: %s", hours_ago,
                     str(error_source_hours))
        else:
            try:
                if not comp:
                    results = validate_instance_level_stats(metrics_client, tracker_url,
                                                            row.topology, row.cluster,
                                                            row.environ,
                                                            source_hours_list,
                                                            future_mins_list,
                                                            metrics_start, metrics_end)
                else:
                    results = validate_component_level_stats(metrics_client, tracker_url,
                                                row.topology, row.cluster,
                                                row.environ,
                                                source_hours_list,
                                                future_mins_list,
                                                metrics_start, metrics_end)
            except Exception as err:
                LOG.error("Error validating topology %s: %s", row.topology,
                          str(err))
            else:
                results["topology"] = row.topology
                results["cluster"] = row.cluster
                results["environ"] = row.environ
                results["user"] = row.user

                save_path: str = os.path.join(
                    output_dir, (f"{row.topology}_{row.cluster}_{row.environ}"
                                 f"_prophet_errors.csv"))
                LOG.info("Saving results to: %s", save_path)
                results.to_csv(save_path)

                if output is None:
                    output = results
                else:
                    output = output.append(results)

    return output

def _create_parser() -> argparse.ArgumentParser:

    parser: argparse.ArgumentParser = argparse.ArgumentParser(
        description=("This program validates the predictions of the Caladrius "
                     "Prophet traffic modelling system for Heron Topologies"))

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
                        help=("Output directory to save results DataFrames "
                              "too."))

    parser.add_argument("-sh", "--source_hours", type=float, required=True,
                        nargs="*",
                        help=("List of source hour durations for prophet "
                              "models to be built against"))

    parser.add_argument("-fm", "--future_mins", type=int, required=True,
                        nargs="*",
                        help=("List of future minute durations for prophet "
                              "models to forecast traffic ahead by"))

    parser.add_argument("-t", "--topologies", type=str, required=False,
                        nargs="*",
                        help=("Optional list of topology names to limit the "
                              "validation to"))

    parser.add_argument("--components", required=False, action="store_true",
                        help=("Optional flag indicating that a model should be built"
                              "for component-level statistics"))

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
            print("\nStarting Caladrius Prophet Model Traffic Validation\n")
            print(f"Loading configuration from file: {ARGS.config}")

    if not os.path.exists(CONFIG["log.file.dir"]):
        os.makedirs(CONFIG["log.file.dir"])

    LOG_FILE: str = CONFIG["log.file.dir"] + "/validation_heron_prophet.log"

    logs.setup(console=(not ARGS.quiet), logfile=LOG_FILE, debug=ARGS.debug)

    # HERON METRICS CLIENT
    METRICS_CLIENT: HeronMetricsClient = \
        loader.get_class(CONFIG["heron.metrics.client"])(
            CONFIG["heron.metrics.client.config"])

    if not os.path.exists(ARGS.output_dir):
        LOG.info("Creating output directory: %s", ARGS.output_dir)
        os.makedirs(ARGS.output_dir)
    else:
        LOG.info("Saving results to existing directory: %s", ARGS.output_dir)

    RESULTS: pd.DataFrame = run(CONFIG["heron.traffic.models.config"],
                                METRICS_CLIENT, ARGS.source_hours,
                                ARGS.future_mins, ARGS.output_dir,
                                topology_ids=ARGS.topologies,
                                comp=ARGS.components)
    if RESULTS is not None:
        SAVE_PATH: str = os.path.join(ARGS.output_dir,
                                      "all_prophet_errors.csv")
        RESULTS.to_csv(SAVE_PATH)

        print("\n\n########\nProphet Errors\n########\n")

        print(RESULTS.groupby(["topology", "source_hours",
                               "future_mins"])["rms_error"].mean().to_string())
    else:
        LOG.error("No Results returned")
