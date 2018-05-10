# Caladrius

Performance modelling system for Distributed Stream Processing Systems (DSPS)
such as [Apache Heron](https://apache.github.io/incubator-heron/) and [Apache
Storm](http://storm.apache.org/).

## Setup

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

The reference gremlin sever can be downloaded from 
[here](https://www.apache.org/dyn/closer.lua/tinkerpop/3.3.2/apache-tinkerpop-gremlin-server-3.3.2-bin.zip).

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
    
## Running Caladrius

### Configuration

All configuration is done via the `yaml` file provided to the `app.py` script
(see section below). This file defines the models run by the various API
endpoints and any connection details, modelling variables or other
configurations they may require.

An example configuration file with sensible defaults is provided in
`config/main.yaml.example`. You should copy this and edit it with your specific
configurations.

### Starting the API Server

The Caladrius API server can be started by running the `app.py` script in the
root directory. This can be run in the appropriate virtual environment using
pipenv (make sure your `python` command points to Python 3):

    $ pipenv run python app.py --config /path/to/config/file

Additional command line arguments are available via:

    $ pipenv run python app.py --help

## Documentation

Documentation for stable releases is hosted on [ReadTheDocs]().

If you want to build the latest documentation then this can be done using
[Sphinx](http://www.sphinx-doc.org/en/master/index.html). Assuming you have
installed the development dependencies above, the docs can be built using the
following commands in the repository root:

    $ pipenv run sphinx-apidoc -f -o docs/source .
    $ cd docs
    $ pipenv run make html

This will place the constructed html documentation in the `docs/build`
directory.

## Security

If you spot any security or other sensitive issues with the software please
report them via the [Twitter HackerOne](https://hackerone.com/twitter) bug
bounty program.
