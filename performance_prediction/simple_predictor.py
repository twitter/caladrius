# Copyright 2018 Twitter, Inc.
# Licensed under the Apache License, Version 2.0
# http://www.apache.org/licenses/LICENSE-2.0

""" This module models different queues and performs relevant calculations for it."""

import datetime as dt
import math
from typing import Any
import json

from caladrius.metrics.client import MetricsClient
from caladrius.graph.gremlin.client import GremlinClient
from caladrius.model.topology.heron.abs_queueing_models import QueueingModels
from caladrius.model.topology.heron.helpers import *
from caladrius.performance_prediction.predictor import Predictor


class SimplePredictor(Predictor):
    def __init__(self, topology_id: str, cluster: str, environ: str,
                 start: [dt.datetime], end: [dt.datetime], tracker_url: str, metrics_client: MetricsClient,
                 graph_client: GremlinClient, queue: QueueingModels, **kwargs: Any):
        super().__init__(topology_id, cluster, environ, start, end, tracker_url,
                         metrics_client, graph_client, queue, **kwargs)
        self.GC_TIME_THRESHOLD = 500  # units --> ms
        self.CPU_LOAD_THRESHOLD = 0.7  # load per core

    def create_new_plan(self) -> json:
        """ Predicts the performance of the new packing plan by 1) finding
        out the performance of the current plan, 2) finding out where the
        new plan is different 3) analysing how that might impact the new
        plan's performance.

        """
        gc_time: pd.DataFrame = self.metrics_client.get_gc_time(self.topology_id,
                                                                self.cluster, self.environ,
                                                                self.start, self.end, **self.kwargs)

        grouped_gc_time: pd.DataFrame = \
            gc_time.groupby(["component", "task"]).mean().reset_index()[["component", "task", "gc-time"]]

        grouped_gc_time.rename(index=str, columns={"gc-time": "av-gc-time"}, inplace=True)

        cpu_load: pd.DataFrame = self.metrics_client.get_cpu_load(self.topology_id, self.cluster,
                                                                  self.environ, self.start, self.end, **self.kwargs)
        grouped_cpu_load: pd.DataFrame = \
            cpu_load.groupby(["component", "task"]).mean().reset_index()[["component", "task", "cpu-load"]]
        grouped_cpu_load.rename(index=str, columns={"cpu-load": "av-cpu-load"}, inplace=True)
        merged: pd.DataFrame = grouped_cpu_load.merge(grouped_gc_time)

        (new_plan, expected_service_rate) = self.process_resource_bottlenecks(merged)

        # now check if parallelism has to be updated
        new_plan: pd.DataFrame = self.process_parallelism(new_plan, expected_service_rate)
        return new_plan.to_json()

    def process_resource_bottlenecks(self, merged: pd.DataFrame) -> (pd.DataFrame, pd.DataFrame):
        """This function is used to determine whether resources should be increased for topology operators
        if they are bottle-necked. Then, expected service rates are updated accordingly."""
        # making a copy of the current plan to modify
        new_plan = self.current_plan.copy()

        temp_merged = merged.copy()
        temp_merged["prop-load"] = (temp_merged["av-cpu-load"]/self.CPU_LOAD_THRESHOLD)

        # processing memory
        temp_merged["prop-time"] = temp_merged["av-gc-time"]/self.GC_TIME_THRESHOLD

        # we find the maximum proportion by which CPU and RAM need to be increased per component
        maximum: pd.DataFrame = temp_merged.groupby("component").max().reset_index()

        # then, we multiply the resources already provisioned by the max proportion
        # they need to be increased by
        for index, row in maximum.iterrows():
            if row["prop-load"] > 1:
                new_plan.loc[new_plan["instance"] == row["component"], "CPU"] =\
                    math.ceil(new_plan.loc[new_plan["instance"] == row["component"]]
                              ["CPU"] * row["prop-load"])
            if row["prop-time"] > 1:
                new_plan.loc[new_plan["instance"] == row["component"], "RAM"] =\
                    math.ceil(new_plan.loc[new_plan["instance"] == row["component"]]
                              ["RAM"] * row["prop-time"])

        # given the above code, we have an updated physical plan but we still need to update the
        # expected service rate, as we expect bottlenecks to be resolved

        # create a copy of the service rates
        expected_service_rate = self.queue.service_rate.copy()

        # this represents the set of tasks whose service rate we're going to update in the current loop
        task_set = set()
        for index, row in new_plan.iterrows():
            tasks = row[["tasks"]][0]  # this is a list of tasks belonging to one component

            if not set(tasks).issubset(task_set):
                # this is the max of the service rates of those tasks
                max_service_rates = pd.DataFrame.max(
                    expected_service_rate.loc[expected_service_rate["task"].isin(tasks)])["mean_service_rate"]

                comp = row["instance"]

                # values can be nan for spouts
                if max_service_rates is not np.nan:
                    prop_time = maximum.loc[maximum["component"] == comp]["prop-time"].iloc[0]
                    prop_load = maximum.loc[maximum["component"] == comp]["prop-load"].iloc[0]

                    min_prop = 0
                    if prop_time > 1 and prop_load > 1:
                        # if we had to increase both CPU and memory resources, we expect a performance improvement
                        # in proportion to the minimum increase.
                        # this is a conservative estimate
                        min_prop = min(prop_time, prop_load)
                    elif prop_load > 1:
                        min_prop = prop_load
                    elif prop_time > 1:
                        min_prop = prop_time

                    if min_prop == 0:
                        min_prop = 1  # so it returns the original result

                    expected_service_rate.loc[expected_service_rate["task"].isin(tasks), "mean_service_rate"] =\
                        max_service_rates * min_prop
                # once we have updated all tasks belonging to a component, there is no need to update any of those
                # tasks again
                task_set.update(tasks)

        return new_plan, expected_service_rate

    def process_parallelism(self, new_plan: pd.DataFrame, expected_service_rate: pd.DataFrame) -> pd.DataFrame:
        """This function takes in the updated packing plan of the topology and the expected service rate
        and uses it to determine if the parallelism level of operators needs to be changed. We can conservatively
        increase the parallelism level, but we do not decrease it."""

        # sum up arrival rate per component
        arrival_rate: pd.DataFrame = self.queue.arrival_rate.copy()

        for index, row in new_plan.iterrows():
            task_arrivals = arrival_rate.loc[arrival_rate["task"].isin(row["tasks"])]

            min_serviced = expected_service_rate.loc[expected_service_rate["task"].isin(row["tasks"])][
                "mean_service_rate"].min()

            if not task_arrivals.empty:
                total_arrivals = task_arrivals["mean_arrival_rate"].sum()

                # we are assuming equal distribution here.
                parallelism = math.ceil(total_arrivals/min_serviced)
                if parallelism > row["parallelism"]:
                    new_plan.loc[index, "parallelism"] = (parallelism)

        return new_plan
