# Copyright 2018 Twitter, Inc.
# Licensed under the Apache License, Version 2.0
# http://www.apache.org/licenses/LICENSE-2.0

""" This module contains abstract base classes for the heron metrics clients
"""
import datetime as dt

from abc import abstractmethod
from typing import Union

from pandas import DataFrame

from caladrius.metrics.client import MetricsClient


class HeronMetricsClient(MetricsClient):
    """ Abstract base class for all Heron metric client classes. """

    @abstractmethod
    def get_service_times(self, topology_id: str, cluster: str, environ: str,
                          start: dt.datetime, end: dt.datetime,
                          **kwargs: Union[str, int, float]) -> DataFrame:
        """ Gets a time series of the service times of each of the bolt
        instances in the specified topology"""
        pass

    @abstractmethod
    def get_receive_counts(self, topology_id: str, cluster: str, environ: str,
                           start: dt.datetime, end: dt.datetime,
                           **kwargs: Union[str, int, float]) -> DataFrame:
        """ Gets a time series of the receive counts of each of the bolt
        instances in the specified topology"""
        pass

    @abstractmethod
    def get_emit_counts(self, topology_id: str, cluster: str, environ: str,
                        start: dt.datetime, end: dt.datetime,
                        **kwargs: Union[str, int, float]) -> DataFrame:
        """ Gets a time series of the emit count of each of the instances in
        the specified topology"""
        pass

    @abstractmethod
    def get_execute_counts(self, topology_id: str, cluster: str, environ: str,
                           start: dt.datetime, end: dt.datetime,
                           **kwargs: Union[str, int, float]) -> DataFrame:
        """ Gets a time series of the emit count of each of the instances in
        the specified topology"""
        pass

    @abstractmethod
    def get_complete_latencies(self, topology_id: str, cluster: str,
                               environ: str, start: dt.datetime,
                               end: dt.datetime,
                               **kwargs: Union[str, int, float]) -> DataFrame:
        """ Gets a time series of the complete latencies of each of the spout
        instances in the specified topology"""
        pass

    @abstractmethod
    def get_calculated_arrival_rates(self, topology_id: str, cluster: str, environ: str,
                                     start: dt.datetime, end: dt.datetime,
                                     **kwargs: Union[str, int, float]) -> DataFrame:
        """ Gets a time series of the arrival rates, in units of tuples per
        second, for each of the instances in the specified topology"""
        pass

    @abstractmethod
    def get_incoming_queue_sizes(self, topology_id: str, cluster: str, environ: str,
                                 start: [dt.datetime] = None, end: [dt.datetime] = None,
                                 **kwargs: Union[str, int, float]) -> DataFrame:
        """ Gets the size of the incoming queue for  each component in the
        topology as a timeseries. The start and end times for the window
        over which to gather metrics, as well as the granularity of the
        time series can also be specified."""
        pass

    @abstractmethod
    def get_cpu_load(self, topology_id: str, cluster: str, environ: str,
                     start: [dt.datetime] = None, end: [dt.datetime] = None,
                     **kwargs: Union[str, int, float]) -> DataFrame:
        """ Gets the CPU load for every running instance in the topology.
        This value is a double in the [0.0,1.0] interval. A value of 0.0
        means that none of the CPUs were running threads from the instance
        during the recent period of time observed, while a value of 1.0 means
        that all CPUs were actively running threads from the JVM
        100% of the time during the recent period being observed."""
        pass

    @abstractmethod
    def get_gc_time(self, topology_id: str, cluster: str, environ: str,
                    start: [dt.datetime] = None, end: [dt.datetime] = None,
                    **kwargs: Union[str, int, float]) -> DataFrame:
        """ Gets the time spent in garbage collection for every running
        instance in the topology."""
        pass

    @abstractmethod
    def get_num_packets_received(self, topology_id: str, cluster: str, environ: str,
                                 start: [dt.datetime] = None, end: [dt.datetime] = None,
                                 **kwargs: Union[str, int, float]) -> DataFrame:
        """ Retrieves the number of packets received (from the stream manager) per instance."""
        pass

    @abstractmethod
    def get_packet_arrival_rate(self, topology_id: str, cluster: str, environ: str,
                                start: [dt.datetime] = None, end: [dt.datetime] = None,
                                **kwargs: Union[str, int, float]) -> DataFrame:
        """ Retrieves the number of packets received (from stream manager) per ms per
        instance."""

        pass

    @abstractmethod
    def get_tuple_arrivals_at_stmgr(self, topology_id: str, cluster: str, environ: str,
                                    start: [dt.datetime] = None, end: [dt.datetime] = None,
                                    **kwargs: Union[str, int, float]) -> DataFrame:
        """ Retrieves the number of packets received at the stream manager for every local
        instance. We use this value to calculate inter-arrival times of packets."""

        pass

    @abstractmethod
    def get_end_to_end_latency(self, topology_id: str, cluster: str, environ: str, sink: str,
                               start: [dt.datetime] = None, end: [dt.datetime] = None,
                               **kwargs: Union[str, int, float]) -> DataFrame:
        """This function is used to obtain end to end latency if the topology reports it.
        The user would currently have to update the topology's code to report such a
        metric."""
        pass

    @abstractmethod
    def get_outgoing_queue_processing_rate(self, topology_id: str, cluster: str, environ: str,
                                           start: [dt.datetime] = None, end: [dt.datetime] = None,
                                           **kwargs: Union[str, int, float]) -> DataFrame:
        """Reports the number of tuples drained from the outgoing queue at
         each instance per minute"""
        pass

    @abstractmethod
    def get_out_going_queue_arrival_rate(self, topology_id: str, cluster: str, environ: str,
                                         start: [dt.datetime] = None, end: [dt.datetime] = None,
                                         **kwargs: Union[str, int, float]) -> DataFrame:
        """Reports the number of tuples added to the outgoing queue of the topology's
        instances per minute"""
        pass

    @abstractmethod
    def get_average_tuple_set_size_added_to_outgoing_queue(self, topology_id: str, cluster: str, environ: str,
                                                     start: [dt.datetime] = None, end: [dt.datetime] = None,
                                                     **kwargs: Union[str, int, float]) -> DataFrame:
        """Reports the number of tuples per tupleset that is added to the outgoing queue per minute of
         an instance"""
        pass
