""" This module contains methods for querying details from a Cuckoo database
"""
from typing import List

import requests

def get_services(zone: str) -> List[str]:
    """ Gets a list of all service names contained within the Cuckoo metrics
    database hosted in the supplied zone.

    Arguments:
        zone (str): The data centre name to pull the service list from.

    Returns:
        A list of service name strings.
    """

    url: str = f"https://cuckoo-prod-{zone}.twitter.biz/services"

    response: requests.Response = requests.get(url)

    response.raise_for_status()

    return response.json()

def get_heron_topology_names(zone: str) -> List[str]:
    """ Gets a list of all heron service (topology) names contained within the
    Cuckoo metrics database hosted in the supplied zone.

    Arguments:
        zone (str): The data centre name to pull the heron service list from.

    Returns:
        A list of heron service (topology) name strings.
    """

    services: List[str] = get_services(zone)

    heron_services: List[str] = [service for service in services
                                 if "heron" in service]

    return heron_services

def get_sources(zone: str, service: str) -> List[str]:
    """ Gets a list of all source names for the supplied service, contained
    within the Cuckoo metrics database hosted in the supplied zone.

    Arguments:
        zone (str): The data centre name to pull the service list from.
        service (str):  The name of the service whose sources are to be listed.

    Returns:
        A list of source name strings.
    """

    url: str = f"https://cuckoo-prod-{zone}.twitter.biz/sources"

    response: requests.Response = requests.get(url,
                                               params={"service" : service})

    response.raise_for_status()

    return response.json()

def get_metrics(zone: str, service: str, source: str) -> List[str]:
    """ Gets a list of all metrics names for the supplied service and source,
    contained within the Cuckoo metrics database hosted in the supplied zone.

    Arguments:
        zone (str): The data centre name to pull the service list from.
        service (str):  The name of the service where the source is running.
        source (str): The name of the source whose metrics are to be listed.

    Returns:
        A list of metric name strings.
    """

    url: str = f"https://cuckoo-prod-{zone}.twitter.biz/metrics"

    response: requests.Response = requests.get(url,
                                               params={"service" : service,
                                                       "source" : source})

    response.raise_for_status()

    return response.json()
