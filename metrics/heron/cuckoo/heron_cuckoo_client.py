""" This module contains methods for querying details from a Cuckoo database
"""
import logging

import datetime as dt

from typing import List, Dict, Union, Any

import requests

import pandas as pd

from caladrius.metrics.heron.heron_metrics_client import HeronMetricsClient

LOG: logging.Logger = logging.getLogger(__name__)

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
    source_comp: str = metrics_list[1]

    details: Dict[str, Any] = {"container" : container_num,
                               "component" : component,
                               "task" : task_id,
                               "source_component" : source_comp}

    if "received-count" in metrics_list[0]:
        details["source_task"] = int(metrics_list[2])
        stream: str = metrics_list[3]
    else:
        stream = metrics_list[2]

    details["stream"] = stream

    return details

class HeronTwitterCuckooClient(HeronMetricsClient):
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
        self.base_url: str = config["database.url"]
        self.default_zone: str = config["default.zone"]

    def get_base_url(self, zone: str = None) -> str:
        """ Gets the base connection URL for the Cuckoo database, using the
        zone argument if supplied. If not the system will use the default_zone
        zone in the config object supplied to the constructor.

        Arguments:
            zone (str): Optional argument to change the data centre name from
                        which to pull the service list from. Default to the
                        "default.zone" key the config object supplied to the
                        constructor.

        Returns:
            The base URL database address
        """

        if zone:
            return self.base_url.format(zone=zone)

        return self.base_url.format(zone=self.default_zone)

    def get_services(self, zone: str = None) -> List[str]:
        """ Gets a list of all service names contained within the Cuckoo
        metrics database hosted in the supplied zone.

        Arguments:
            zone (str): Optional argument to change the data centre name from
                        which to pull the service list from. Default to the
                        "default.zone" key the config object supplied to the
                        constructor.

        Returns:
            A list of service name strings.
        """

        url: str = self.get_base_url(zone) + "services"

        response: requests.Response = requests.get(url)

        response.raise_for_status()

        return response.json()

    def get_heron_topology_names(self, zone: str = None) -> List[str]:
        """ Gets a list of all heron service (topology) names contained within the
        Cuckoo metrics database hosted in the supplied zone.

        Arguments:
            zone (str): The data centre name to pull the heron service list from.

        Returns:
            A list of heron service (topology) name strings.
        """

        services: List[str] = self.get_services(zone)

        heron_services: List[str] = [service for service in services
                                     if "heron" in service]

        return heron_services

    def get_sources(self, service: str, zone: str = None) -> List[str]:
        """ Gets a list of all source names for the supplied service, contained
        within the Cuckoo metrics database hosted in the supplied (or default)
        zone.

        Arguments:
            service (str):  The name of the service whose sources are to be
                            listed.
            zone (str): Optionally you can supply the data centre name to pull
                        the service list from. If not supplied then the default
                        zone from the config object file will be used.

        Returns:
            A list of source name strings.
        """

        url: str = self.get_base_url(zone) + "sources"

        response: requests.Response = requests.get(
            url, params={"service" : service})

        response.raise_for_status()

        return response.json()

    def get_metrics(self, service: str, source: str,
                    zone: str = None) -> List[str]:
        """ Gets a list of all metrics names for the supplied service and
        source, contained within the Cuckoo metrics database hosted in the
        supplied (or default) zone.

        Arguments:
            service (str):  The name of the service where the source is running.
            source (str): The name of the source whose metrics are to be listed.
            zone (str): Optionally you can supply the data centre name to pull
                        the service list from. If not supplied then the default
                        zone from the config object file will be used.

        Returns:
            A list of metric name strings.
        """

        url: str = self.get_base_url(zone) + "metrics"

        response: requests.Response = requests.get(url,
                                                   params={"service" : service,
                                                           "source" : source})

        response.raise_for_status()

        return response.json()

    def query(self, query_str: str, query_name: str, granularity: str,
              start: int = None, end: int = None, zone: str = None)  -> dict:
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
            zone (str): Optional data centre zone name. If not supplied the
                        default zone, as defined in the config object, will be
                        used.

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
        url: str = self.get_base_url(zone) + "query"

        payload: Dict[str, str] = {"query" : query_str,
                                   "client_source" : self.client_name,
                                   "granularity" : granularity,
                                   "name" : query_name}

        if start:
            payload["start"] = start
        if start and not end:
            LOG.debug("Start time (%d) supplied with no corresponding end "
                      "time. Time period will be from the start time to the "
                      "current time", start)

        if start and end:
            payload["end"] = end
        elif end and not start:
            LOG.warning("End time (%d) supplied without corresponding start "
                        "time. This will be ignored and the default time "
                        "window used for this query", end)

        response: requests.Response = requests.get(url, params=payload)

        # Raise an exception if the status code is not OK
        response.raise_for_status()

        json_response: Dict[str, Any] = response.json()

        # Check that the query ran successfully
        if json_response["status"] != "Success":
            raise RuntimeError(f'Query return non-successful status: '
                               f'{response["Success"]}')

        return json_response


    def get_service_times(self, topo_name: str, component_name: str,
                          granularity: str = "m", start: int = None,
                          end: int = None, zone: str = None) -> pd.DataFrame:
        """ Gets the service times, as a timeseries, for every instance of the
        specified component of the specified topology. The start and end times
        for the window over which to gather metrics can be specified.

        Arguments:
            topo_name (str):    The topology identification string.
            component_name (str):   The name of the component whose metrics are
                                    required.
            granularity (str):  The time granularity for this query, can be one
                                of "h", "m", "s". Defaults to "m".
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
            zone (str): Optional data centre zone name. If not supplied the
                        default zone, as defined in the config object, will be
                        used.

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

        query_str: str = (f"ts(avg, heron/{topo_name}, /{component_name}/*, "
                          f"__execute-latency/*/*)")

        LOG.debug("Querying for execute latency using query: %s", query_str)

        json_response: requests.Response = self.query(query_str,
                                                      "execute latency",
                                                      granularity, start, end,
                                                      zone)

        output: List[Dict[str, Any]] = []

        for instance in json_response["timeseries"]:
            details: Dict[str, Any] = parse_metric_details(instance["source"])
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

    def get_execute_count(self, topo_name: str, component_name: str,
                          granularity: str = "m", start: int = None,
                          end: int = None, zone: str = None) -> pd.DataFrame:
        """ Gets the execution counts, as a timeseries, for every instance of
        the specified component of the specified topology. The start and end
        times for the window over which to gather metrics can be specified.

        Arguments:
            topo_name (str):    The topology identification string.
            component_name (str):   The name of the component whose metrics are
                                    required.
            granularity (str):  The time granularity for this query, can be one
                                of "h", "m", "s". Defaults to "m".
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
            zone (str): Optional data centre zone name. If not supplied the
                        default zone, as defined in the config object, will be
                        used.

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
        query_str: str = (f"ts(sum, heron/{topo_name}, /{component_name}/*, "
                          f"__execute-count/*/*)")

        LOG.debug("Querying for execute count using query: %s", query_str)

        json_response: requests.Response = self.query(query_str,
                                                      "execute count",
                                                      granularity, start, end,
                                                      zone)

        output: List[Dict[str, Any]] = []

        for instance in json_response["timeseries"]:
            details: Dict[str, Any] = parse_metric_details(instance["source"])
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

    def get_received_count(self, topo_name: str, component_name: str,
                           granularity: str = "m", start: int = None,
                           end: int = None, zone: str = None) -> pd.DataFrame:
        """ Gets the tuple received counts, as a timeseries, for every instance
        of the specified component of the specified topology. The start and end
        times for the window over which to gather metrics can be specified.

        *NOTE*: This is not (yet) a default metric supplied by Heron so a
        custom metric class must be used in your topology to provide this.

        Arguments:
            topo_name (str):    The topology identification string.
            component_name (str):   The name of the component whose metrics are
                                    required.
            granularity (str):  The time granularity for this query, can be one
                                of "h", "m", "s". Defaults to "m".
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
            zone (str): Optional data centre zone name. If not supplied the
                        default zone, as defined in the config object, will be
                        used.

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
                source_task:    The Instance ID number for the instance that
                                the tuples that produced this metric were sent
                                from.
                stream: The name of the incoming stream from which the tuples
                        that lead to this metric came from.
                received_count:  The tuples received count for this instance.
        """

        query_str: str = (f"ts(heron/{topo_name}, /{component_name}/*, "
                          f"received-count/*/*/*)")

        LOG.debug("Querying for receive count using query: %s", query_str)

        json_response: requests.Response = self.query(query_str,
                                                      "received counts",
                                                      granularity,
                                                      start, end, zone)

        output: List[Dict[str, Any]] = []

        for instance in json_response["timeseries"]:
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
                    "received_count" : entry[1]}
                output.append(row)

        return pd.DataFrame(output)
