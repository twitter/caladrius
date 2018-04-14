# Caladrius

Performance modelling system for Distributed Stream Processing Systems (DSPS)
such as [Apache Heron](https://apache.github.io/incubator-heron/) and [Apache
Storm](http://storm.apache.org/).

## Requirements

### Python

Caladrius requires Python 3.5+, additional Python dependencies are listed in
the Pipfile. Dependencies can be installed using
[pipenv](https://docs.pipenv.org/) by running the following command in the
caladrius root directory:

    $ pipenv install 

Add the `--dev` flag to the above command to install development dependencies.

### Graph Database

Caladrius requires a [Gremlin
Server](http://tinkerpop.apache.org/docs/current/reference/#gremlin-server)
instance running [TinkerPop](http://tinkerpop.apache.org/) 3.3.2 or higher. 

The reference gremlin sever can be downloaded from [here](https://www.apache.org/dyn/closer.lua/tinkerpop/3.3.2/apache-tinkerpop-gremlin-server-3.3.2-bin.zip).

The Gremlin server should have the [Gremlin
Python](http://tinkerpop.apache.org/docs/current/reference/#gremlin-python)
plugin installed:

    $ gremlin-server.sh install org.apache.tinkerpop gremlin-python 3.3.2

Start the server with the gremlin python config (included in the standard
server distribution):

    $ gremlin-server.sh start conf/gremlin-server-modern-py.yaml

*Please note:* The default settings for the Gremlin Server result in an
in-memory TinkerPop Server instance. If graphs need to be persisted to disk
then these settings can be altered in the appropriate configuration file in the
`conf` directory of the Gremlin Server distribution.

## Documentation

The Caladrius API documentation is built using
[Sphinx](http://www.sphinx-doc.org/en/master/index.html). Assuming you have
installed the development dependencies above, the docs can be built using the
following commands:

    $ pipenv run sphinx-apidoc -f -o docs/source .
    $ cd docs
    $ pipenv run make html

This will place the constructed html documentation in the `docs/build`
directory.
    
## Start-Up

The Caladrius API server can be started by running the `app.py` script in the
root directory. This can be run in the appropriate virtual environment using
pipenv:

    $ pipenv run python app.py

*Please note:* The configuration loading code is not complete yet so the
configuration values for each of the required classes need to be supplied
manually via a config dictionary with the appropriate keys.
