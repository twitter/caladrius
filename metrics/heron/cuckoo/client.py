""" This module contains methods for querying details from a Cuckoo database
"""
import logging

import datetime as dt

from typing import List, Dict, Union, Any, cast

import requests

import pandas as pd

from caladrius.metrics.heron.client import HeronMetricsClient

LOG: logging.Logger = logging.getLogger(__name__)

#pylint: disable=too-many-arguments, too-many-locals

def convert_dt_to_ts(dt_object: dt.datetime) -> int:
    """ Helper method to convert datetime object into cuckoo integer
    timestamps. """

    return int(round(dt_object.timestamp()))

def parse_metric_details(details: Dict[str, List[str]]) -> Dict[str, Any]:
    """ Helper method for extracting details from metric name strings.

    Arguments:
        details (dict): dictionary of metric details returned by the cuckoo
                        API

    Returns:
        A dictionary with the following keys: "container", "component", "task",
        "source_component".
    """

    ref_list: List[str] = details["sources"][0].split("/")
    component: str = ref_list[1]

    instance_list: List[str] = ref_list[2].split("_")
    container_num: int = int(instance_list[1])
    task_id: int = int(instance_list[3])


    metrics_list: List[str] = details["metrics"][0].split("/")

    instance_details: Dict[str, Any] = {"container" : container_num,
                                        "component" : component,
                                        "task" : task_id}

    if "receive-count" in metrics_list[0]:
        instance_details["source_component"] = metrics_list[1]
        instance_details["source_task"] = int(metrics_list[2])
        stream: str = metrics_list[3]
    elif "emit-count" in metrics_list[0]:
        stream = metrics_list[1]
    else:
        stream = metrics_list[2]
        instance_details["source_component"] = metrics_list[1]

    instance_details["stream"] = stream

    return instance_details

class HeronCuckooClient(HeronMetricsClient):
    """ Class for extracting heron metrics from the Cuckoo timeseries database.
    """

    def __init__(self, config: Dict[str, Union[str, int, float]],
                 client_name: str) -> None:
        """ Constructor for Twitter Cuckoo Database connection client.

        Arguments:
            config (dict):  Dictionary containing the metrics client
                            configuration values.
            client_name (str):  An string used to identify the request this
                                client issues to the metrics database.
        """
        super().__init__(config)

        self.client_name: str = client_name
        self.base_url: str = cast(str, config["cuckoo.database.url"])

    def get_services(self) -> List[str]:
        """ Gets a list of all service names contained within the Cuckoo
        metrics database.

        Returns:
            A list of service name strings.
        """

        url: str = self.base_url + "/services"

        response: requests.Response = requests.get(url)

        response.raise_for_status()

        return response.json()

    def get_heron_topology_names(self) -> List[str]:
        """ Gets a list of all heron service (topology) names contained within
        the Cuckoo metrics database.

        Returns:
            A list of heron service (topology) name strings.
        """

        services: List[str] = self.get_services()

        heron_services: List[str] = [service for service in services
                                     if "heron" in service]

        return heron_services

    def get_sources(self, service: str) -> List[str]:
        """ Gets a list of all source names for the supplied service, contained
        within the Cuckoo metrics database.

        Arguments:
            service (str):  The name of the service whose sources are to be
                            listed.

        Returns:
            A list of source name strings.
        """

        url: str = self.base_url + "/sources"

        response: requests.Response = requests.get(
            url, params={"service" : service})

        response.raise_for_status()

        return response.json()

    def get_metrics(self, service: str, source: str) -> List[str]:
        """ Gets a list of all metrics names for the supplied service and
        source, contained within the Cuckoo metrics database hosted in the
        supplied (or default) zone.

        Arguments:
            service (str):  The name of the service where the source is running.
            source (str): The name of the source whose metrics are to be listed.

        Returns:
            A list of metric name strings.
        """

        url: str = self.base_url + "/metrics"

        response: requests.Response = requests.get(url,
                                                   params={"service" : service,
                                                           "source" : source})

        response.raise_for_status()

        return response.json()

    def query(self, query_str: str, query_name: str, granularity: str,
              start: int = None, end: int = None)  -> Dict[str, Any]:
        """ Run the supplied query against the cuckoo database.

        Arguments:
            query_str (str): The CQL query string.
            query_name (str): A reference string for this query.
            granularity (str):   The time granularity for this query, can be one
                                of "h", "m", "s".
            start (int):    Optional start time for the time period the query
                            is run against. This should be UNIX time integer
                            (seconds since epoch). If not supplied then the
                            default time window (e.g. the last 2 hours) will be
                            used for the supplied query. If only the start time
                            is supplied then the period from the start time to
                            the current time will be used,
            end (int):  Optional end time for the time period the query
                        is run against. This should be UNIX time integer
                        (seconds since epoch). This should not be supplied
                        without a corresponding start time. If it is, it will
                        be ignored.

        Returns:
            A dictionary parsed from the JSON string returned by the get
            request to the Cuckoo database.

        Raises:
            requests.HTTPError: If the request returns anything other than a
                                2xx status code.
            RuntimeError:   If the request went through correctly but the
                            database was unable to run the request
                            successfully. This will usually happen if there is
                            a syntax error with the query string.
        """
        url: str = self.base_url + "/query"

        payload: Dict[str, Union[str, int]] = \
            {"query" : query_str, "client_source" : self.client_name,
             "granularity" : granularity, "name" : query_name}

        if start:
            payload["start"] = start
        if start and not end:
            LOG.info("Start time (%d) supplied with no corresponding end "
                     "time. Time period will be from the start time to the "
                     "current time", start)

        if start and end:
            payload["end"] = end
        elif end and not start:
            LOG.warning("End time (%d) supplied without corresponding start "
                        "time. This will be ignored and the default time "
                        "window used for this query", end)

        if not start and not end:
            LOG.info("No start or end time supplied for this query so "
                     "default window used for this query")


        response: requests.Response = requests.get(url, params=payload)

        # Raise an exception if the status code is not OK
        response.raise_for_status()

        json_response: Dict[str, Any] = response.json()

        # Check that the query ran successfully
        if json_response["status"] != "Success":
            raise RuntimeError(f'Query return non-successful status: '
                               f'{json_response["Success"]}')

        return json_response


    def get_service_times(self, topology_id: str, start: dt.datetime = None,
                          end: dt.datetime = None,
                          **kwargs: Union[str, int, float]) -> pd.DataFrame:
        """ Gets the service times, as a timeseries, for every instance of
        every bolt component of the specified topology. The start and end times
        for the window over which to gather metrics, as well as the granularity
        of the time series can also be specified.

        Arguments:
            topology_id (str):    The topology identification string.
            start (dt.datetime):    Optional start time for the time period the
                                    query is run against. This should be a UTC
                                    datetime object. If not supplied then the
                                    default time window (e.g. the last 2 hours)
                                    will be used for the supplied query. If
                                    only the start time is supplied then the
                                    period from the start time to the current
                                    time will be used,
            end (dt.datetime):  Optional end time for the time period the query
                                is run against. This should be UTC datetime
                                object. This should not be supplied without a
                                corresponding start time. If it is, it will be
                                ignored.
            **granularity (str):  Optional time granularity for this query, can
                                  be one of "h", "m", "s". Defaults to "m".

        Returns:
            A pandas DataFrame containing the service time measurements as a
            timeseries. Each row represents a measurement (averaged over the
            specified granularity) with the following columns:
                timestamp:  The UTC timestamp for the metric.
                component: The component this metric comes from.
                task:   The instance ID number for the instance that the metric
                        comes from.
                container:  The ID for the container this metric comes from.
                source_component:   The name of the component that issued the
                                    tuples that resulted in this metric.
                stream: The name of the incoming stream from which the tuples
                        that lead to this metric came from.
                latency_ms: The execute latency measurement in milliseconds.
        """

        query_str: str = (f"ts(avg, heron/{topology_id}, /*/*, "
                          f"__execute-latency/*/*)")

        if start:
            start_ts: int = convert_dt_to_ts(start)
        else:
            start_ts = None

        if end:
            end_ts: int = convert_dt_to_ts(end)
        else:
            end_ts = None

        if start and end:
            LOG.info("Querying for execution latencies for topology: %s over "
                     "a period of %d seconds from %s to %s UTC", topology_id,
                     (end-start).total_seconds(), start.isoformat(),
                     end.isoformat())

        if "granularity" not in kwargs:
            granularity: str = "m"
        else:
            granularity = cast(str, kwargs["granularity"])

        json_response: Dict[str, Any] = self.query(query_str,
                                                   "execute latency",
                                                   granularity, start_ts,
                                                   end_ts)

        output: List[Dict[str, Any]] = []

        for instance in json_response["timeseries"]:

            # Ignore returned values from the system components such as
            # "__stmgr__" as they do not have execute-latency stats
            if "__" in instance["source"]["sources"][0]:
                continue
            else:
                details: Dict[str, Any] = \
                    parse_metric_details(instance["source"])

                for entry in instance["data"]:
                    row: Dict[str, Any] = {
                        "timestamp" : dt.datetime.utcfromtimestamp(entry[0]),
                        "component" : details["component"],
                        "task" : details["task"],
                        "container" : details["container"],
                        "source_component" : details["source_component"],
                        "stream" : details["stream"],
                        "latency_ms" : entry[1] / 1000}
                    output.append(row)

        return pd.DataFrame(output)

    def get_execute_counts(self, topology_id: str, start: dt.datetime = None,
                           end: dt.datetime = None,
                           **kwargs: Union[str, int, float]) -> pd.DataFrame:
        """ Gets the execution counts, as a timeseries, for every instance of
        the every component in the specified topology. The start and end
        times for the window over which to gather metrics can be specified.

        Arguments:
            topology_id (str):    The topology identification string.
            start (dt.datetime):    Optional start time for the time period the
                                    query is run against. This should be a UTC
                                    datetime object. If not supplied then the
                                    default time window (e.g. the last 2 hours)
                                    will be used for the supplied query. If
                                    only the start time is supplied then the
                                    period from the start time to the current
                                    time will be used,
            end (dt.datetime):  Optional end time for the time period the query
                                is run against. This should be UTC datetime
                                object. This should not be supplied without a
                                corresponding start time. If it is, it will be
                                ignored.
            **granularity (str):  Optional time granularity for this query, can
                                  be one of "h", "m", "s". Defaults to "m".

        Returns:
            A pandas DataFrame containing the execution count measurements as a
            timeseries. Each row represents a measurement (summed over the
            specified granularity) with the following columns:
            timestamp:  The UTC timestamp for the metric.
            component: The component this metric comes from.
            task:   The instance ID number for the instance that the metric
                    comes from.
            container:  The ID for the container this metric comes from.
            source_component:   The name of the component that issued the
                                tuples that resulted in this metric.
            stream: The name of the incoming stream from which the tuples
                    that lead to this metric came from.
            execute_count:  The execution count for this instance.
        """

        query_str: str = (f"ts(sum, heron/{topology_id}, /*/*, "
                          f"__execute-count/*/*)")

        if start:
            start_ts: int = convert_dt_to_ts(start)
        else:
            start_ts = None

        if end:
            end_ts: int = convert_dt_to_ts(end)
        else:
            end_ts = None

        if start and end:
            LOG.info("Querying for execution counts for topology: %s over a "
                     "period of %d seconds from %s to %s UTC", topology_id,
                     (end-start).total_seconds(), start.isoformat(),
                     end.isoformat())

        if "granularity" not in kwargs:
            granularity: str = "m"
        else:
            granularity = cast(str, kwargs["granularity"])

        json_response: Dict[str, Any] = self.query(query_str,
                                                   "execute count",
                                                   granularity, start_ts,
                                                   end_ts)

        output: List[Dict[str, Any]] = []

        for instance in json_response["timeseries"]:

            # Ignore returned values from the system components such as
            # "__stmgr__" as they do not have execute-latency stats
            if "__" in instance["source"]["sources"][0]:
                continue
            else:
                details: Dict[str, Any] = \
                    parse_metric_details(instance["source"])
                for entry in instance["data"]:
                    row: Dict[str, Any] = {
                        "timestamp" : dt.datetime.utcfromtimestamp(entry[0]),
                        "component" : details["component"],
                        "task" : details["task"],
                        "container" : details["container"],
                        "source_component" : details["source_component"],
                        "stream" : details["stream"],
                        "execute_count" : entry[1]}
                    output.append(row)

        return pd.DataFrame(output)

    def get_emit_counts(self, topology_id: str, start: dt.datetime = None,
                        end: dt.datetime = None,
                        **kwargs: Union[str, int, float]) -> pd.DataFrame:
        """ Gets the emit counts, as a timeseries, for every instance of
        the every component in the specified topology. The start and end
        times for the window over which to gather metrics can be specified.

        Arguments:
            topology_id (str):    The topology identification string.
            start (dt.datetime):    Optional start time for the time period the
                                    query is run against. This should be a UTC
                                    datetime object. If not supplied then the
                                    default time window (e.g. the last 2 hours)
                                    will be used for the supplied query. If
                                    only the start time is supplied then the
                                    period from the start time to the current
                                    time will be used,
            end (dt.datetime):  Optional end time for the time period the query
                                is run against. This should be UTC datetime
                                object. This should not be supplied without a
                                corresponding start time. If it is, it will be
                                ignored.
            granularity (str):  Optional time granularity for this query, can
                                be one of "h", "m", "s". Defaults to "m".

        Returns:
            A pandas DataFrame containing the emit count measurements as a
            timeseries. Each row represents a measurement (summed over the
            specified granularity) with the following columns:
                timestamp:  The UTC timestamp for the metric.
                component: The component this metric comes from.
                task:   The instance ID number for the instance that the metric
                        comes from.
                container:  The ID for the container this metric comes from.
                source_component:   The name of the component that issued the
                                    tuples that resulted in this metric.
                stream: The name of the incoming stream from which the tuples
                        that lead to this metric came from.
                emit_count:  The emit count for this instance.
        """

        query_str: str = (f"ts(sum, heron/{topology_id}, /*/*, "
                          f"__emit-count/*)")

        if start:
            start_ts: int = convert_dt_to_ts(start)
        else:
            start_ts = None

        if end:
            end_ts: int = convert_dt_to_ts(end)
        else:
            end_ts = None

        if start and end:
            LOG.info("Querying for emit counts for topology: %s over a "
                     "period of %d seconds from %s to %s UTC", topology_id,
                     (end-start).total_seconds(), start.isoformat(),
                     end.isoformat())

        if "granularity" not in kwargs:
            granularity: str = "m"
        else:
            granularity = cast(str, kwargs["granularity"])

        json_response: Dict[str, Any] = self.query(query_str, "emit counts",
                                                   granularity, start_ts,
                                                   end_ts)

        output: List[Dict[str, Any]] = []

        for instance in json_response["timeseries"]:

            # Ignore returned values from the system components such as
            # "__stmgr__" as they do not have execute-latency stats
            instance_tag: str = instance["source"]["sources"][0]
            if "__" in instance_tag:
                LOG.debug("Skipping emit count metrics for system element: "
                          "%s", instance_tag)

            # Some of the metrics for emit counts are empty (they refer to
            # emissions onto incoming streams ?!?) so ignore them
            if not instance["data"]:
                LOG.debug("Skipping empty emit count metric % for element: %s",
                          instance_tag, instance["source"]["metrics"][0])
                continue

            details: Dict[str, Any] = \
                parse_metric_details(instance["source"])
            for entry in instance["data"]:
                row: Dict[str, Any] = {
                    "timestamp" : dt.datetime.utcfromtimestamp(entry[0]),
                    "component" : details["component"],
                    "task" : details["task"],
                    "container" : details["container"],
                    "stream" : details["stream"],
                    "emit_count" : int(entry[1])}
                output.append(row)

        return pd.DataFrame(output)

    def get_receive_counts(self, topology_id: str, start: dt.datetime = None,
                           end: dt.datetime = None,
                           **kwargs: Union[str, int, float]) -> pd.DataFrame:
        """ Gets the tuple receive counts, as a timeseries, for every instance
        of every component in the specified topology. The start and end
        times for the window over which to gather metrics can be specified.

        *NOTE*: This is not (yet) a default metric supplied by Heron so a
        custom metric class must be used in your topology to provide this.

        Arguments:
            topology_id (str):    The topology identification string.
            start (dt.datetime):    Optional start time for the time period the
                                    query is run against. This should be a UTC
                                    datetime object. If not supplied then the
                                    default time window (e.g. the last 2 hours)
                                    will be used for the supplied query. If
                                    only the start time is supplied then the
                                    period from the start time to the current
                                    time will be used,
            end (dt.datetime):  Optional end time for the time period the query
                                is run against. This should be UTC datetime
                                object. This should not be supplied without a
                                corresponding start time. If it is, it will be
                                ignored.
            **granularity (str):    The time granularity for this query, can be
                                    one of "h", "m", "s". Defaults to "m".

        Returns:
            A pandas DataFrame containing the execution count measurements as a
            timeseries. Each row represents a measurement (summed over the
            specified granularity) with the following columns:
            timestamp:  The UTC timestamp for the metric.
            component: The component this metric comes from.
            task:   The Instance ID number for the instance that the metric
                    comes from.
            container:  The ID for the container this metric comes from.
            source_component:   The name of the component that issued the
                                tuples that resulted in this metric.
            source_task:    The Instance ID number for the instance that the
                            tuples that produced this metric were sent from.
            stream: The name of the incoming stream from which the tuples that
                    lead to this metric came from.
            receive_count:  The tuples received count for this instance.
        """

        query_str: str = (f"ts(heron/{topology_id}, /*/*, "
                          f"receive-count/*/*/*)")

        if start:
            start_ts: int = convert_dt_to_ts(start)
        else:
            start_ts = None

        if end:
            end_ts: int = convert_dt_to_ts(end)
        else:
            end_ts = None

        if start and end:
            LOG.info("Querying for receive count for topology: %s over a "
                     "period of %d seconds from %s to %s UTC", topology_id,
                     (end-start).total_seconds(), start.isoformat(),
                     end.isoformat())

        if "granularity" not in kwargs:
            granularity: str = "m"
        else:
            granularity = cast(str, kwargs["granularity"])

        json_response: Dict[str, Any] = self.query(query_str,
                                                   "receive-counts",
                                                   granularity, start_ts,
                                                   end_ts)

        output: List[Dict[str, Any]] = []

        for instance in json_response["timeseries"]:

            # If this entry is for a system component such as a "__stmgr__"
            # then skip it
            instance_tag: str = instance["source"]["sources"][0]
            if "__" in instance_tag:
                LOG.debug("Skipping receive count metrics for system element: "
                          "%s", instance_tag)
                continue

            LOG.debug("Processing receive metrics for instance: %s",
                      instance_tag)
            details: Dict[str, Any] = parse_metric_details(instance["source"])
            for entry in instance["data"]:
                row: Dict[str, Any] = {
                    "timestamp" : dt.datetime.utcfromtimestamp(entry[0]),
                    "component" : details["component"],
                    "task" : details["task"],
                    "container" : details["container"],
                    "source_component" : details["source_component"],
                    "source_task" : details["source_task"],
                    "stream" : details["stream"],
                    "receive_count" : int(entry[1])}
                output.append(row)

        return pd.DataFrame(output)
