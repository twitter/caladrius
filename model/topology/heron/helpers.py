""" This file contains helper functions. """

import logging

import pandas as pd

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
