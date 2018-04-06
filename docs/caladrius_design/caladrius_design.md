[//]: # (This document is written in Pandoc Markdown format)

# Introduction

This is the design document for the Distributed Stream Processing Topology
modelling service *Caladrius*^[This is the Roman name for the legend of the
healing bird that takes sickness into itself, the ancient Greek version of this
is called *Dhalion*]. The aim of this service is to accept a physical plan (a
mapping of component instances to logical containers --- called a packing plan
in Heron) for a stream processing topology and use metrics from that running
topology to predict its performance if it were to be configured according to
the proposed plan.

# System Overview

The proposed layout for the system is shown below:

![Caladrius System Overview](./imgs/caladrius_overview.png){#fig:system-overview}

# Caladrius API

Caladrius will provide several REST endpoints to allow clients to query the
various modelling systems it provides. Initially Caladrius will provide
topology performance (end-to-end/complete latency, throughput etc) and traffic
(incoming workload) modelling services. Other services such as topology graph
analysis could be added later.

The general format of the Caladrius end points is: `/model/<aspect being
modelled>/<DSPS name>/<current or proposed>/<topology id>`. Examples endpoints
are given below:

## Topology Performance

* `POST /model/toplogy/heron/proposed/{topology-id}` --- This request will
  model the proposed packing plan which is contained within the `POST`
  request's body. The payload should be the protobuf serialised [packing 
  plan](https://github.com/apache/incubator-heron/blob/master/heron/proto/packing_plan.proto)
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

### Asynchronous request/response

It is important to consider that a call to the topology modelling endpoints may
incur a significant wait (up to several seconds, depending on the modelling
logic). Therefore it seems prudent to design the API to be asynchronous,
allowing the client to continue with other operations while the modelling is
completed. Also, having an asynchronous API also means that making the
calculation pipeline, on the server side, run concurrently should be easier.

A call to the modelling endpoints will return a reference code (model ID) for
the proposed plan being calculated. The client can then send a GET request to
the original request URL, with the model ID as a parameter, to see if the
calculation in complete. If it is not the client will receive a "pending"
response, when the calculation is complete the JSON modelling results will
posted at that URL. This also has the advantage of providing clients with a way
to query past modelling runs without re-running the calculations. An example is
given below:

``` 
POST /model/topology/heron/WordCount1

RESPONSE 202 model_id=1234

GET /model/topology/heron/WordCount1?model_id=1234
```

### Response

The response format from the topology performance modelling API will contain
the results of the performance prediction for the provided packing plan. The
response will be a JSON formatted string containing the results of the
modelling. The type of results listed will vary by model implementation.

However, certain fields will be common to all returned JSON objects:

```json
{
    'model_id' : 1234
    'prediction_method': 'Queuing Theory',
    'requested' : '2018-04-05T01:34:56.770483',
    'completed' : '2018-04-05T01:35:01.871964',
    'topology_id': 'WordCount1',
    'results': {
        'latency' : {
            'units' : 'ms'
            'mean' : 124.25
            'max' : 356.24
            'min' : 95.56
        }
        'throughput' : {
            'units' : 'tps'
            'mean' : 1024
            'max' : 2096
            'min' : 256
        }
    }
}
```

## Traffic Prediction

### Request

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

### Response

The response format for the traffic predictions will vary according to the
implementation of the `TrafficModel` interface. However, certain key summary
statistics will be included in all responses:

```json
{
    'prediction_method': 'average',
    'requested': '2018-04-05T01:34:56.770483',
    'completed': '2018-04-05T01:34:58.770483',
    'topology_id': 'WordCount1',
    'results': {
        'arrival_rate': {
            'mean' : 150 
            'max' : 323,
            'min' : 102
        }
    }
}
```

# Topology Performance Modelling Interface {#sec:topo-performance}

Caladrius will be able to run one or more models against the proposed packing
plan(s). Each instance of the model interface will accept the Metrics (see
@sec:metrics) and Graph (see @sec:graph) interfaces and will use their custom
code to calculate the expected performance. The `TopologyModel` interface is
shown below:

```python
class TopologyModel(ABC):
    """ Abstract base class for all topology performance modelling classes """

    @abstractmethod
    def __init__(self, metrics: MetricsClient, graph: GraphClient) -> None:
        self.metrics = metrics
        self.graph = graph

    @abstractmethod
    def predict_performance(self, topology_id: str,
                            proposed_plan: Any) -> dict:
        """ Predicts the performance of the specified topology when configured
        according to the proposed physical plan.

        Arguments:
            topology_id (str):  The identification string for the topology
                                whose performance will be predicted.
            proposed_plan:  A data structure containing the proposed physical
                            plan.

        Returns:
            A dictionary (suitable for conversion to JSON) containing the
            performance prediction.
        """
        pass
```

# Traffic Modelling Interface {#sec:traffic}

Similar to the topology performance modelling interface (see 
@sec:topo-performance), there is an abstract base class for predicting traffic into the topologies:

```python
class TrafficModel(ABC):
    """ Abstract base class for all traffic modelling classes """

    @abstractmethod
    def __init__(self, metrics: MetricsClient, graph: GraphClient) -> None:
        self.metrics = metrics
        self.graph = graph

    @abstractmethod
    def predict_traffic(self, topology_id: str, duration: int) -> dict:
        """ Predicts the expected traffic arriving at the specified topology
        over the period defined by the duration argument.

        Arguments:
            topology_id (str):  The identification string for the topology
                                whose traffic will be predicted.
            duration (int): The number of minuets over which to summarise the
                            traffic predictions. For example duration = 120
                            would equate to "predict the traffic over the next
                            two hours".

        Returns:
            A dictionary (suitable for conversion to JSON) containing the
            traffic prediction.
        """
        pass
```

At it simplest a concrete implementation of traffic model could take the arrival rate into the specified topology over the last hour and return an average as that as a predictor for the next hour.

# Metrics Interface {#sec:metrics}

The Metrics interface will provide methods for accessing and summarising
performance metrics from a given metrics source. For example concrete implementations could allow metrics to be extracted from the Heron Topology Master metrics API or the Cuckoo timeseries database.

There is a master `MetricsClient` abstract base class which is the superclass
for each of the DSPS metric client interfaces e.g. `HeronMetrics`,
`StormMetrics` etc. The DSPS metric client interfaces define the methods
required to model topologies and traffic for each of the supported DSPSs. 

# Graph Interface {#sec:graph}

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

The controller is the main process for Caladrius. It is responsible for reading in the configuration files (see @sec:config)

# Configuration Files {#sec:config}

Caladrius will be highly configurable, all the modelling, metrics and graph processing code can be specified via YAML files in the `/config` directory. 

# Proposed Work Plan

The system proposed above is quite complex and represents the end goal for the
Caladrius service. Initially the aim will be to provide a single calculation pipeline with a single implementation of each interface and a single topology model. 

The proposed sequence of work is listed below:

1) 
