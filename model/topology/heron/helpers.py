""" This file contains helper functions. """

import logging

import pandas as pd
import numpy as np
from typing import Dict, List

LOG: logging.Logger = logging.getLogger(__name__)


def convert_throughput_to_inter_arr_times(arrivals_per_min: pd.DataFrame) -> pd.DataFrame:
    task_arrivals: pd.DataFrame = arrivals_per_min.groupby(["task"])

    df: pd.DataFrame = pd.DataFrame(columns=['task', 'mean_inter_arrival_time', 'std_inter_arrival_time'])

    for row in task_arrivals:
        data = row[1]
        # inter-arrival time = time in ms divided by number of tuples received in that time
        time = (60.0 * 1000)/data["num-tuples"]
        df = df.append({'task': row[1]["task"].iloc[0],
                       'mean_inter_arrival_time': time.mean(), 'std_inter_arrival_time': time.std()},
                       ignore_index=True)

    return df


def process_execute_latencies(execute_latencies: pd.DataFrame) -> pd.DataFrame:
    latencies: pd.DataFrame = execute_latencies.groupby(["task"])

    df: pd.DataFrame = pd.DataFrame(columns=['task', 'mean_service_time', 'std_service_time'])

    for row in latencies:
        data = row[1]
        latencies = data["latency_ms"]
        df = df.append({'task': row[1]["task"].iloc[0],
                        'mean_service_time': latencies.mean(), 'std_service_time': latencies.std()},
                       ignore_index=True)

    return df


def convert_service_times_to_rates(latencies: pd.DataFrame) -> pd.DataFrame:
    grouped_latencies: pd.DataFrame = latencies.groupby(["task"])
    df: pd.DataFrame = pd.DataFrame(columns=['task', 'mean_service_rate'])

    for row in grouped_latencies:
        data = row[1]
        latencies = data["latency_ms"]
        df = df.append({'task': row[1]["task"].iloc[0],
                        'mean_service_rate': 1/latencies.mean()}, ignore_index=True)

    return df


def convert_arr_rate_to_mean_arr_rate(throughput: pd.DataFrame) -> pd.DataFrame:
    grouped_throughput: pd.DataFrame = throughput.groupby(["task"])
    df: pd.DataFrame = pd.DataFrame(columns=['task', 'mean_arrival_rate'])
    # per minute
    for row in grouped_throughput:
        data = row[1]
        throughput = data["num-tuples"]/(60.0 * 1000)
        df = df.append({'task': row[1]["task"].iloc[0],
                        'mean_arrival_rate': throughput.mean()}, ignore_index=True)

    return df


def find_end_to_end_latencies(paths: List[List[str]], waiting_times: pd.DataFrame, service_times: pd.DataFrame) -> list:
    """
    This function goes through all end to end paths in the
    topology (from source to sink) and calculates the total end to
    end latency for each path. This end to end latency is a summation of
    execute latency + queue waiting time for each bolt in the path.
    :param paths: All end to end topology paths from source to sink
    :param waiting_times: The amount of time each tuple has to wait
    in an instance's queue
    :param service_times: The amount of time it takes an instance
    to process a tuple
    :return: a json list of end to end latencies for each path in the topology
    """
    averaged_execute_latency = service_times[["task", "latency_ms"]].groupby("task").mean().reset_index()
    merged = averaged_execute_latency.merge(waiting_times, on=["task"])[["task", "mean_waiting_time", "latency_ms"]]

    result = dict()

    for path in paths:
        end_to_end_latency: np.float64 = 0.0
        for x in range(len(path)):
            row = merged.loc[(merged["task"] == path[x])]
            end_to_end_latency = row["latency_ms"].tolist()[0] + row["mean_waiting_time"].tolist()[0] + end_to_end_latency

        result[tuple(path)] = end_to_end_latency

    return remap_keys(result)


def remap_keys(latencies_dict: Dict[tuple, np.float64]):
    return [{'path': k, 'latency': v} for k, v in latencies_dict.items()]


def validate_queue_size(execute_counts: pd.DataFrame, tuple_arrivals: pd.DataFrame) -> pd.DataFrame:
    """
    This function approximates the queue size per instance at the stream manager
    by simply looking at how many tuples are processed per minute and how many tuples
    arrive per minute. Roughly, the size of the queue should be (tuples arrived from
    the last minute + the current minute - tuples executed in the current minute). This
    is only meant to be a rough estimate to validate the queue sizes returned by
    the queueing theory models
    :param execute_counts: number of tuples executed per instance
    :param tuple_arrivals: number of tuples that have arrived at
    the stream manager per instance
    :return:
    """
    merged: pd.DataFrame = execute_counts.merge(tuple_arrivals, on=["task", "timestamp"])[["task","execute_count","num-tuples", "timestamp"]]
    merged["rough-diff"] = merged["num-tuples"] - merged["execute_count"].astype(np.float64)

    grouped = merged.groupby(["task"])

    df: pd.DataFrame = pd.DataFrame(columns=['task', 'actual-queue-size', 'timestamp'])
    for row in grouped:
        diff = 0
        for x in range(len(row[1])):
            if x == 0:
                diff = row[1]["num-tuples"].iloc[x]
            elif x == len(row[1]) - 1:
                diff = diff - row[1]["execute_count"].iloc[x].astype(np.float64)
            else:
                diff = diff + row[1]["num-tuples"].iloc[x] - row[1]["execute_count"].iloc[x].astype(np.float64)

            df = df.append({'task': row[1]["task"].iloc[0],
                            'timestamp': row[1]["timestamp"].iloc[x],
                            'actual-queue-size': diff}, ignore_index=True)

    LOG.info(df.groupby("task")[["actual-queue-size"]].mean())
    return merged
