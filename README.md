# Caladrius

Performance modelling system for Distributed Stream Processing Systems (DSPS)
such as [Apache Heron](https://apache.github.io/incubator-heron/) and [Apache
Storm](http://storm.apache.org/).

## Requirements

### Python

Caladrius requires Python 3.5+, see PipFile for additional Python dependencies.
Dependencies can be installed using [pipenv](https://docs.pipenv.org/) by
running the following command in the caladrius root directory:

    $ pipenv install 

### Graph Database

Caladrius requires a [Gremlin
Server](http://tinkerpop.apache.org/docs/current/reference/#gremlin-server)
instance running [TinkerPop](http://tinkerpop.apache.org/) 3.3.2 or higher. The
Gremlin server should have the [Gremlin
Python](http://tinkerpop.apache.org/docs/current/reference/#gremlin-python)
plugin installed:

    $ gremlin-server.sh install org.apache.tinkerpop gremlin-python 3.3.2

Start the server with:

    $ gremlin-server.sh conf/gremlin-server-modern-py.yaml

