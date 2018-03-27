""" Module containing methods for obtaining metrics for the elements of a given
topology."""
import logging

from typing import Dict, List, Any

import datetime as dt

import requests

import pandas as pd

LOG: logging.Logger = logging.getLogger(__name__)

def query(query_str: str, client_source: str, query_name: str, granuality: str,
          zone: str, start: int = None, end: int = None) -> dict:

    url: str = f"https://cuckoo-prod-{zone}.twitter.biz/query"

    payload: Dict[str, Any] = {"query" : query_str,
                               "client_source" : client_source,
                               "granularity" : granuality,
                               "name" : query_name}

    if start:
        payload["start"] = start

    if end:
        payload["end"] = end

    response: requests.Response = requests.get(url, params=payload)

    LOG

    # Raise an exception if the status code is not OK
    response.raise_for_status()

    json_response: Dict[str, Any] = response.json()

    # Check that the query ran successfully
    if json_response["status"] != "Success":
        raise RuntimeError(f'Query return non-successful status: '
                           f'{response["Success"]}')

    return json_response

def parse_metric_details(details: Dict[str, List[str]]) -> Dict[str, Any]:

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

def get_service_time_df(topo_name: str, component_name: str,
                        client_source: str, query_name: str,
                        zone: str, granuality: str = "m", start: int = None,
                        end: int = None) -> pd.DataFrame:

    query_str: str = (f"ts(heron/{topo_name}, /{component_name}/*, "
                      f"__execute-latency/*/*)")

    LOG.debug("Querying for execute latency using query: %s", query_str)

    json_response: requests.Response = query(query_str, client_source,
                                             query_name, granuality,
                                             zone, start, end)

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

def get_execute_count_df(topo_name: str, component_name: str,
                         client_source: str, query_name: str,
                         zone: str, granuality: str = "m", start: int = None,
                         end: int = None) -> pd.DataFrame:

    query_str: str = (f"ts(heron/{topo_name}, /{component_name}/*, "
                      f"__execute-count/*/*)")

    LOG.debug("Querying for execute count using query: %s", query_str)

    json_response: requests.Response = query(query_str, client_source,
                                             query_name, granuality,
                                             zone, start, end)

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
                "execute-count" : entry[1]}
            output.append(row)

    return pd.DataFrame(output)

def get_receive_count_df(topo_name: str, component_name: str,
                         client_source: str, query_name: str,
                         zone: str, granuality: str = "m", start: int = None,
                         end: int = None) -> pd.DataFrame:

    query_str: str = (f"ts(heron/{topo_name}, /{component_name}/*, "
                      f"received-count/*/*/*)")

    LOG.debug("Querying for receive count using query: %s", query_str)

    json_response: requests.Response = query(query_str, client_source,
                                             query_name, granuality,
                                             zone, start, end)

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
                "execute-count" : entry[1]}
            output.append(row)

    return pd.DataFrame(output)
