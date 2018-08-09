Models
======

Caladrius provides various models than can be used to predict the performance 
of DSPS topologies. These fall into two main categories:

.. _traffic-models:

Traffic
-------

Traffic models predict the expected amount of workload arriving into a topology
at some point in the future. These predictions can be passed to the
`Topology Performance`_ Models to allow the possible effect of these traffic
levels to predicted.

The currently available traffic models are:

**Heron**:

    * :doc:`/models/traffic/heron/stats_summary`: A basic statistical summary 
    * :doc:`/models/traffic/heron/prophet`: Times series prediction using the
      Facebook Prophet library

.. _topology-models:

Topology Performance
--------------------

Topology performance models allow the effect of different traffic levels, on
a currently running topology to be predicted.

The currently available performance models are:

**Heron**:
    
    * :doc:`/models/topology/heron/queueing_theory`: A performance model based
      on queuing theory

