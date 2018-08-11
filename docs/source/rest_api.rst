REST API
========

Caladrius exposes several REST endpoints over which to request performance
modelling. 

Model
-----

Traffic
~~~~~~~

.. _traffic_model_info:

:code:`GET /model/traffic/{dsps-name}/model_info`

Returns:
    A JSON list of objects each with a :code:`name` and :code:`decription`
    attribute for the configured traffic models. The names are unique across
    Caladrius and can be used in the :code:`model` fields of other requests.

:code:`GET /model/traffic/{dsps-name}`

Parameters:
    :code:`topology_id`
        Required - The topology identification string
    :code:`cluster`
        Required - The cluster the topology is running on 
    :code:`environ`
        Required - The environment the cluster is running in ("prod", "devel", 
        etc)
    :code:`model`
        Required - Repeated - One or more models to be run. These names should
        match the names returned by the 
        :ref:`model information <traffic_model_info>` endpoint.
        Alternatively, :code:`all` can be supplied to run all configured traffic
        models.
    :code:`source_hours`
        Required - The number of hours of source data to use in the traffic 
        model predictions. 
    
Topology
~~~~~~~~

.. _topology_model_info:

:code:`GET /model/topology/{dsps-name}/model_info`

Returns:
    A JSON list of objects each with a :code:`name` and :code:`decription`
    attribute for the configured topology performance models. The names are
    unique across Caladrius and can be used in the :code:`model` fields of
    other requests.

:code:`POST /model/topology/{dsps-name}/current`
    
Parameters:
    :code:`topology_id`
        Required - The topology identification string
    :code:`cluster`
        Required - The cluster the topology is running on 
    :code:`environ`
        Required - The environment the cluster is running in ("prod", "devel", 
        etc)
    :code:`model`
        Required - Repeated - One or more models to be run. These names should
        match the names returned by the 
        :ref:`model information <topology_model_info>` endpoint.
        Alternatively, :code:`all` can be supplied to run all configured traffic
        models.
