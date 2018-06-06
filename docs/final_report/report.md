[//]: # (This document is written in Pandoc Markdown format)

# Introduction

This is the summary report for the Caladrius internship project. It summarises
the project, progress made against the original design document^[See *Caladrius
--- Design Document* April 10th, 2018], preliminary modelling results and
discusses future work.

For more details on the background of the problem area, API documentation and
further discussion on how to modify and contribute new models to Caladrius,
please consult the documentation in the `/docs` folder of the Caladrius
repository.

## Original Aim

The original aim of the Caladrius service was to accept a physical plan (a
mapping of component instances to logical containers --- called a packing plan
in Heron) for a stream processing topology and use metrics from that running
topology to predict its performance if it were to be configured according to
the proposed plan. It was also intended that Caladrius would provide other
modelling services such as traffic prediction (arrival rate into a topology).

## Revised Aims

Due to issues with how Heron changes instance identifiers after an update, it
is not currently possible to predict the routing of tuples for key/field based
stream groupings across a topology update. Planned changes to Heron's stateful
processing should make this kind of prediction possible in the future. However,
for the time being, the project had to focus on a different objective.

The new aim was to produce a system that could forecast the expected traffic
level into a topology and then construct of model of tuple flow through the
topology's current layout and predict the expected arrival rate at each instance with the forecast traffic level. In this way potential issues with back pressure could be identified ahead of time.

In future, once instance identification is consistent across topology updates, additional modelling can be added to caladrius to predict performance with a proposed plan.

# System Overview {#sec:system-overview}

The layout for the current Caladrius implementation is shown in
@fig:system-overview. This shows the currently implemented metric clients, traffic and topology models as well as other significant modules.

![Caladrius System
  Overview](./imgs/caladrius_overview.png){#fig:system-overview}

# Traffic Prediction

The performance of a topology is function of the amount of tuples that enter via the various spout instances. As spouts operate on a *pull* model, where they actively fetch data from an external source, we define *traffic* as the emit count (in a given sample period) from each spout instance.

The emit counts per time period for each spout instance form a time series.
Depending on the topology application this time series may be relatively random
and exhibit little or no pattern, like that shown in @fig:random-timeseries.
Alternatively, topologies based on user interactions such as incoming tweets, can exhibit strong seasonality such as that shown in
@fig:seasonal-timeseries.

![48 hours of emit count data from an example word count topology with a random
  emission profile. Emissions are the average from all instances of the spout
  component aggregated into 10 minuet
  samples.](./imgs/random_timeseries.png){#fig:random-timeseries}

![48 hours of emit count data from a production topology (BubbleHeron) that is
  based on tweets. Emissions are the average from all instances of the spout
  component aggregated into 10 minuet
  samples.](./imgs/seasonal_timeseries.png){#fig:seasonal-timeseries}

Time series forecasting is a complex field of research and many methods for
predicting future trends from past data exist. For random timeseries, like
those shown in @fig:random-timeseries, a simple statistics summary (mean,
median, etc.) of the last $T$ data points may be sufficient for a reasonable
forecast. To this end, Caladrius provides a `TrafficModel` implementation:
`StatsSummaryTrafficModel`^[see `/model/traffic/heron/stats_summary.py` in the
Caladrius repository for implementation details]. This model and its associated
REST endpoint will summarise a user defined period of historical traffic data
and provide mean, median, quantile^[quantile levels are configurable via the
main configuration file] and other summary statistics. 

However, while the `StatsSummaryTrafficModel` is simple to implement and is
fast to return results, strongly seasonal time series like that shown in
@fig:seasonal-timeseries, is not well predicted by such an approach. For
example, a mean of the last 2 hours of data in @fig:seasonal-timeseries may be
a good predictor of the next hours traffic at that time of day. However, if the
full 48 hours of previous data were used or 2 hours where the traffic level was
dropping, then the mean would be a significant over estimation.

To deal with seasonality more sophisticated modelling techniques are required.
A full investigation of the appropriate statistical techniques for predicting
traffic levels of the Heron topologies at Twitter was beyond the scope of this
project. However, there exist several open source software packages that aim to
provide generalised model building for time series data. One such packages,
from Facebook, is Prophet.

## Prophet

[Prophet](https://facebook.github.io/prophet/) is a procedure for forecasting
time series data. It is based on an additive model where non-linear trends are
fit with periodic (yearly, weekly, daily, etc) seasonality^[The package is
based on a research paper: Taylor SJ, Letham B. (2017) Forecasting at scale.
PeerJ Preprints 5:e3190v2
[https://doi.org/10.7287/peerj.preprints.3190v2](https://doi.org/10.7287/peerj.preprints.3190v2)].
It works best with daily periodicity data with at least one year of historical
data, however will work with lower levels of historic data. Prophet is robust
to missing data, shifts in the trend, and large outliers.

Caladrius provides a `TrafficModel` implementation: `ProphetTrafficModel`^[see
`/model/traffic/heron/prophet.py` in the Caladrius repository for
implementation details] that uses the Prophet forecasting package. This model
and its associated REST endpoint allow the user to specify a period of historic
data from which models will be built. The user can specify weather a single model for each spout-component/output-stream combination (faster, but potentially less accurate) or separate models for each spout-instance/output-stream (slower, but gives per instance results) should be built. The user also specifies how much time in the future should be forecast. 

Once a future forecast is produced various summary statistics (like those produced by the `StatsSummaryModel` are produced from it. In this way the user can answer questions such as "What is expected average traffic level over the next hour?" or "What is the maximum expected traffic level over the next 30 minutes?"

![Illustration of historic training data and resulting model created by the
  Prophet package on 7 days of per minute emit count data from the BubblerHeron
  toplogy](./imgs/Long_Term_Prophet.png){#fig:long-term-model}

![Close up of the final hours of training data from the BubblerHeron toplogy,
  showing the predicted emit count over the next 30
  minutes](./imgs/Short_Term_Prophet.png){#fig:short-term-model}

@Fig:long-term-model shows an example of a Prophet model trained on 7 days worth of emit count data from the *BubblerHeron* topology. It shows the data point (black dots), the model derived from them (dark blue line) and the error around that model (light blue area). @Fig:short-term-model shows a close up for the last few hours of the historical training data along with a prediction of the next 30 minuets of emit count data.

## Validation

# Back pressure prediction


## Graph Interface {#sec:graph}

Topologies can be represented as directed graphs^[As an aside, it is often said
that Storm/Heron topologies are Directed Acyclic Graphs (DAGs), this is not
correct. It is perfectly possible to have a downstream bolt connect back to an
upstream bolt thereby forming a cycle. However, while possible, it is rare. But
many Machine Learning algorithms incorporate feedback loops so we should avoid
using DAG to describe topologies]. Many of the possible modelling techniques
for topology performance involve analysing these graphs. Caladrius will provide
a graph database interface where topology logical and physical plans can be
uploaded and used for performance analysis. 

The graph interface is based on [Apache
TinkerPop](http://tinkerpop.apache.org/), which is an abstraction layer over
several popular graph databases. This means that the graph database back-end
can be changed if needed (to better serve the needs of a particular model)
without having to reimplement the graph interface code.

Caladrius will provide classes for accessing logical and physical plan
information from the DSPS Information APIs and convert this via
TinkerPop/Gremlin code into directed graph representations. It will also
provide a graph prediction (see @sec:graph-prediction below) interface for
estimating properties of proposed physical plans.

Again the Graph Client implementations can be set via the configuration files
along with implementation specific configuration options (see @sec:config).

## The Physical Graph


## Routing Probabilities


### Shuffle Grouped Connections


### Fields Grouped Connections


## Validation


# Caladrius API {#sec:api}

