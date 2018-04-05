[//]: # (This document is written in Pandoc Markdown format)

# Introduction

This is the design document for the Distributed Stream Processing Topology
modelling service *Caladrius*^[This is the Roman name for the legend of the
healing bird that takes sickness into itself, the ancient Greek version of this
is called *Dhalion*]. The aim of this service is to accept a physical plan (a
mapping of component instances to logical containers --- called a packing plan
in Heron) for a topology and use metrics from that running topology to predict
its performance if it were to be configured according to the proposed plan.

# System Overview

The proposed layout for the system is shown below:

![Caladrius System Overview](./imgs/caladrius_overview.png){#fig:system-overview}

## System Operation

In order to model a proposed topology physical plan, the protocol buffer
`packing_plan` message is issued to the Caladrius API. The API then passes the
plan, along with instances of the Metrics and Graph interfaces to one or more
Model implementations^[In future which model to use could be included in the
request. For now it will just be configured on the Caladrius side].

The Model instances then use their custom code along with data provided by the
Metrics and Graph interfaces to calculate the expected performance. The results
of this modelling are then reported via a protocol buffer `modelling_results`
message^[Alternatively this could be a simple JSON message].

# Caladrius API

Caladrius exposes a REST endpoint that will accept a serialised physical
/ packing plan as part of GET message. This packing plan will be deserialised
and passed to the Controller which will process the proposed plan. Caladrius
will accept requests on several different end points depending on the modelling
requirements.

## Endpoints

The general format of the Caladrius end points is: `/model/<aspect being
modelled>/<DSPS name>/<current or proposed>/<topology id>`. Examples endpoints
are given below:

### Topology Performance

* `POST /model/toplogy/heron/proposed/{topology-id}` --- This request will
  model the proposed packing plan which is contained within the `POST`
  request's body. The payload should be the protobuf serialised packing plan
  message issued by Heron. 

* `POST /model/topology/storm/proposed/{topology-id}` --- This request will
  model the proposed physical plan which is contained within the POST request's
  body. The payload should be the serialised WorkerSlots^[Alternatively we
  could define a protocol buffer or JSON schema for Storm packing plans] from
  the Storm Scheduler instance handling the rebalance commands. 

* `GET /model/topology/heron/current/{topology-id}` --- Issuing this request
  will model the performance of the currently deployed physical plan of the
  specified Heron topology using the supplied traffic rate (input into the
  topology). The `traffic` parameter can either be a single value (tuples per
  second) which will be applied to every spout instance or individual traffic
  rates for each spout instance can be supplied by using their `TaskID`s as
  keys:

       ```
        GET /model/heron/current/wordcount1?traffic=150
       ```
       
       ```
        GET /model/heron/current/wordcount1?10=141&11=154&12=149
       ```

* `GET /model/storm/current/{topology-id}` --- Issuing this request will model
  the performance of the currently deployed physical plan of the specified
  Apache Storm topology using the supplied traffic rate (input into the
  topology). The `traffic` parameter can either be a single value (tuples per
  second) which will be applied to every spout executor or individual traffic
  rates for each spout executor can be supplied by using their `TaskID` ranges
  as keys:

       ```
        GET /model/heron/topology/current/wordcount1?traffic=150
       ```
       
       ```
        GET /model/heron/topology/current/wordcount1?10-12=141&13-15=154&16-18=149
       ```

#### Response

The response format from the API will contain the results of the performance
prediction for provided packing plan. The response will be a JSON formatted
string containing the results of the modelling. The type of results listed will
vary by model implementation.

However, certain fields will be common to all returned JSON objects:

### Traffic Prediction

#### Request

* `GET /model/traffic/<heron or storm>/{topology-id}` --- Issuing this request
  will trigger a prediction of the expected traffic for the supplied Heron or
  Storm topology. Parameters include `duration` which indicates how much time
  in the future the traffic prediction should cover. As well as duration,
  a `units` parameter can be passed which can be `h`,`m` or `s` (for hours,
  minuets or seconds). If no no unit parameter is supplied the value of
  duration is assumed to be in minutes. The example below is asking for
  a prediction of incoming traffic into the Heron `WordCount1` topology over
  the next 2 hours:

       ```
        GET /model/traffic/heron/WordCount1?duration=2&units=h
       ```

#### Response

The response format for the traffic predictions will vary according to the implementation of the `TrafficModel` interface. However, certain key summary 
statistics will be included in all responses:

```json
{
    'prediction method': 'default',
    'timestamp': '2018-04-05T01:34:56.770483',
    'topology_id': 'WordCount1',
    'results': {
        'average_arrival_rate': 150,
        'max_arrival_rate' : 323,
        'min_arrival_rate' : 102
    }
}
```

### Asynchronous responses

It is important to consider that a call to the modelling endpoints may incur
a significant wait (up to several seconds, depending on the modelling logic). Therefore it seems prudent to design the API to be asynchronous. Having an asynchronous API also means that making the calculation pipeline run concurrently should be easier.

A call to the modelling endpoints will return a reference code for the proposed
plan being calculated. The client can then query the original request URL plus
this calculation code to see if the calculation in complete, when it is the
JSON response will posted at that URL.

# Model Interface

Caladrius will be able to run one or more Models against the proposed packing
plan(s). Each instance of the model interface will accept the Metrics and Graph
Interfaces (see below) and will use their custom code to calculate the expected
performance.

# Metrics Interface

The Metrics interface will provide methods for accessing and summarising
performance metrics from a given metrics source. Initially implementations for
the Topology Master and Cuckoo based metrics will be provided but
implementations for the other metrics sinks could easily be created.

# Graph Interface

Topologies can be represented as directed graphs^[As an aside, it is often said
that Storm/Heron topologies are Directed Acyclic Graphs (DAGs), this is not
correct. It is perfectly possible to have a downstream bolt connect back to an
upstream bolt thereby forming a cycle. However, while possible, it is rare. But
many Machine Learning algorithms incorporate feedback loops so we should avoid
using DAG to describe topologies]. Many of the possible modelling techniques
for topology performance involve analysing these graphs. Caladrius provides
a graph database interface where topology logical and physical plans can be
uploaded and used for performance analysis. 

The graph interface is based on [Apache
TinkerPop](http://tinkerpop.apache.org/), which is an abstraction layer over
several popular graph databases. This means that the graph database back-end
can be changed if needed (to better serve the needs of a particular model)
without having to reimplement the graph interface code.

Caladrius will provide classes for accessing logical and physical plan
information from the Heron Tracker API and converting this via
TinkerPop/Gremlin code into a directed graph representations. It will also
provide a graph.prediction interface for estimating properties of proposed
physical plan such as instance to instance routing probabilities.

The Model implementation may then use the Graph interface to analyse these
graphs.

# Controller {#sec:controller}

# Configuration Files {#sec:config}



# Language

All the main components of Caladrius will be written in Python^[Python here
refers to Python 3 not Python 2 or Legacy Python as it is now known]. The
Metric, Graph and Model interfaces will be provided via abstract base classes
so that other implementations can be provided in future.

The Caladrius API will be a REST API and so in theory could be implemented in
any language. The use of TinkerPop for the graph interface also means that the
graph databased service can be accessed in any TinkerPop supported language.

# Proposed Work Plan

The system proposed above is quite complex and represents the end goal for the
Caladrius service. Initially the aim will be to provide a single calculation pipeline with a single implementation of each interface and a single topology model. 

The proposed sequence of work is listed below:

1) 
