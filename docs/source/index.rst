Caladrius
=========

Caladrius is a performance modelling service for Distributed Stream Processing
Systems (DSPS) such as Apache Heron_ and Apache Storm_. 

TL;DR
-----

The aim of Caladrius is shorten the time taken to scale a topology to the
correct size to cope with a given incoming workload (traffic level). With this
system, it is hoped, that the *plan -> deploy -> stablise -> analyse* loop that
is currently required to *right size* a topology, can be shortened.

The ability to predict future workloads also allows the performance of
a running topology to be checked against expected high workloads and
pre-emptively scaled before the workload arrives.

Further details are provided in the `Summary`_ and :doc:`/background`
sections.

Prototype
---------

Caladrius is a prototype project and only implements some the features
described above and below. Details of what features have been implemented and
what are planned in future are shown on the :doc:`/roadmap`.

Summary
-------

Caladrius provides a framework to analyse, model and predict the various
aspects of DSPS systems (like Apache Heron and Storm) and focuses on two key
areas:

:ref:`traffic-models`:
    The prediction of incoming workload into a DSPS topology. Caladrius
    provides metrics database interfaces and methods for analysing traffic into
    a topology and predicting future traffic levels.

:ref:`topology-models`:
    Predicting how a topology will perform under a given traffic load and for
    a given cluster layout (physical plan). This breaks down into two further
    areas:

    *Current*:
        The prediction of how a topology will perform under a different traffic
        level if it keeps its current layout.

    *Proposed*:
        The prediction of how a topology will perform under the current (or
        a future) traffic level when its layout is changed.


Table of contents
-----------------

.. toctree::
    :maxdepth: 3
    
    background
    contributing
    graph_database
    rest_api
    models/model_index
    roadmap
    API Documentation <modules>

Indices and tables
------------------

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

.. _Heron: https://apache.github.io/incubator-heron/
.. _Storm: http://storm.apache.org/
