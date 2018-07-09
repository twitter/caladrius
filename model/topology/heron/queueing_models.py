# Copyright 2018 Twitter, Inc.
# Licensed under the Apache License, Version 2.0
# http://www.apache.org/licenses/LICENSE-2.0

""" This module models different queues and performs relevant calculations for it."""

import pandas as pd
import numpy as np

from caladrius.model.topology.heron.abs_queueing_models import QueueingModels
from caladrius.model.topology.heron.helpers import *

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
    def __init__(self, d: dict):
        """
        This function initializes relevant variables to calculate queue related metrics
        given an M/M/c model.
        Arguments:
                service_times (pd.DataFrame) : This number indicates the average
                service time of tuples per instance (calculated by averaging execute
                latency) for the instance.
                arrival_rate (pd.DataFrame): This is a timeseries that indicates the
                number of tuples arriving per millisecond for each instance.
        """
        service_times: pd.DataFrame = d["service_times"]
        arrival_rate: pd.DataFrame = d["arrival_rate"]

        self.service_rates = convert_service_times_to_rates(service_times)
        self.arrival_rate = convert_arr_rate_to_mean_arr_rate(arrival_rate)

    def average_waiting_time(self) -> pd.DataFrame:
        merged: pd.DataFrame = self.service_rates.merge(self.arrival_rate, on=["task"])
        merged["mean_waiting_time"] = merged["mean_arrival_rate"] /\
                                             (merged["mean_service_rate"] *
                                             (merged["mean_service_rate"] - merged["mean_arrival_rate"]))
        return merged

    def average_queue_size(self, actual_queue_size: pd.DataFrame=pd.DataFrame) -> pd.DataFrame:
        merged: pd.DataFrame = self.service_rates.merge(self.arrival_rate, on=["task"])
        merged["server_utilization"] = merged["mean_arrival_rate"]/merged["mean_service_rate"]
        merged["mmc-queue-sizes"] = (merged["server_utilization"] ** 2)/(1 - merged["server_utilization"])

        if not actual_queue_size.empty:
            df: pd.DataFrame = pd.DataFrame(columns=['task', 'actual_queue_size'])
            queue_size = actual_queue_size.groupby(["task"])
            for row in queue_size:
                data = row[1]["queue-size"]
                # convert measured queue size from queue size per minute to queue size per ms
                df = df.append({'task': row[1]["task"].iloc[0],
                                'actual_queue_size': data.mean()/(60*1000)}, ignore_index=True)

            merged = merged.merge(df, on=["task"])

        return merged


class GGCQueue(QueueingModels):
    """
    This class in effect models an GG1 Queue. The G/G/1 queue represents the queue length
    in a system with a single server where interarrival times have a general (or arbitrary)
    distribution and service times have a (different) general distribution. This system can fit
    more realistic scenarios as arrival rates and processing rates do not necessarily fit probabilistic
    distributions (such as the Poisson distribution, used to describe arrival rates in M/M/1 queues).
    """
    def __init__(self, d: dict):
        """
        This function initializes relevant variables to calculate queue related metrics
        given a G/G/c model
        As both data arrival distributions and processing distributions are general,
        it is difficult to find an exact value for the waiting time. However, we can find
        a probable upperbound.
        There is a great deal of detail available here
        http://home.iitk.ac.in/~skb/qbook/Slide_Set_12.PDF and
        http://www.math.nsc.ru/LBRT/v1/foss/gg1_2803.pdf
        Arguments:
            tuple_arrivals (pd.DataFrame): the number of tuples per instance that arrive at the stream
            manager per minute
            service_times (pd.DataFrame): this number indicates the average service time of tuples per
            instance (calculated by averaging execute latency) for the instance.
        """

        self.tuple_arrivals: pd.DataFrame = d["tuple_arrivals"]
        self.service_times: pd.DataFrame = d["service_times"]
        self.execute_counts: pd.DataFrame = d["execute_counts"]
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

        self.queue_size["scaled_queue_size"] = self.queue_size["queue-size"] * 60 * 1000 # because it was calculated per ms
        LOG.info(self.queue_size[["task", "mean_waiting_time", "mean_arrival_rate", "scaled_queue_size", "queue-size"]])
        self.validate_queue_size()

        return self.queue_size

    def validate_queue_size(self) -> pd.DataFrame:

        merged: pd.DataFrame = self.execute_counts.merge(self.tuple_arrivals, on=["task", "timestamp"])[["task","execute_count","num-tuples", "timestamp"]]
        merged["rough-diff"] = merged["num-tuples"] - merged["execute_count"].astype(np.float64)

        grouped = merged.groupby(["task"])

        df: pd.DataFrame = pd.DataFrame(columns=['task', 'queue_size', 'timestamp'])
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
                                'queue_size': diff}, ignore_index=True)

        LOG.info(df.groupby("task")[["queue_size"]].mean())
        return merged

