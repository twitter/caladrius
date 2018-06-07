Contributing
============

Contributions to Caladrius are very welcome. The systems is designed to be
extensible and the aim of the project is to provide a comprehensive suite of
performance models to suit a wide range of situations.

Contributing new models
-----------------------

In order to construct a new model class for a particular DSPS you will need to
inherit from the appropriate base class. For example, to construct a new
traffic prediction model for Heron topologies you would inherit from the
:code:`HeronTrafficModel` class located in the
:code:`model/traffic/heron/base.py` module.

All traffic and topology performance model base classes in Caladrius inherit
from the :code:`Model` base class (see :code:`model/base.py`). This class has
two attributes that your new model class should override:

    - *name*: This is the identification string used across the DSPS sub-domain
      of the Caladrius REST API to refer to this model. It should be distinct
      (within the sub-domain) from all other model names, e.g. `stats-summary`
      is unique within the :code:`model/topology/heron` and :code:`model/traffic/heron`
      endpoints. If the Caladrius server is already running you can check the
      :code:`/model_info` endpoints of each DSPS sub-domain to see list of all
      registered models. Alternatively, once the model is added to the
      configuration file, Caladrius will check for model name uniqueness at
      startup and raise an exception of the name is a duplicate.
    - *description*: This is a short description of what the model does that is
      included in the information returned by the relevant :code:`/model_info`
      endpoint.

The :code:`Model` classes constructor accepts 3 positional arguments that are
given to all models during start up:

    - *config*: This is a dictionary parsed from the appropriate entry in the
      `yaml` configuration file. For example to specify configuration entries
      for a Heron topology performance model class you would add keys and
      values to the :code:`heron.topology.models.config` map within the main
      Caladrius configuration file. Note that any entry in this map will be
      passed to all Heron topology models.
    - *metrics_client*: This is the metrics client the was configured for the
      particular DSPS that this model applies to. For example for Heron traffic
      and topology performance models this would be the metrics client
      specified by the :code:`heron.metrics.client` configuration option.
    - *graph_client*: This the client connected to the Caladrius graph database
      and can be used to query topology structures and write new structures to
      the database (see :doc:`graph_database`).

Your model constructor should accept the 3 positional arguments, any other
required setup arguments should be supplied via the :code:`config` dictionary.

You should consult the documentation for each DSPS's base model base class for
details of the required methods and arguments. These are usually specific to
the DSPS and are supplied to the model by the relevant :code:`Resource` class
in the :code:`api/model` module.

Telling Caladrius about your model
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Once you have a new model class it is easy to add this to Caladrius. The model
classes used by each endpoint are defined in the :code:`yaml` config file
passed to the :code:`app.py` script. An example configuration showing how to
specify the model classes used is shown in :code:`config/main.yaml.example`.

For example if you want to add a new traffic model for Heron topologies you
would add the import path to you model class under the appropriate config
heading::
    
    heron.traffic.models:
        - "caladrius.model.traffic.heron.stats_summary.StatsSummaryTrafficModel"
        - "caladrius.model.traffic.heron.prophet.ProphetTrafficModel"
        - "path.to.my.model.file.MyNewTrafficModel"

The config file uses absolute import paths for each class. Therefore, provided
that your source files are reachable on the :code:`PYTHONPATH` (with
:code:`__init__.py` in required subdirectories) your model does not need to be
within the Caladrius root folder. However, you will need to add the folder
above the Caladrius repo to your :code:`PYTHONPATH` to access the abstract base
classes and other utility features therein.

Once you have added your model to the config file it will be loaded by the REST
endpoint resource class and will be available at the appropriate endpoint. You
can query the model by supplying its :code:`name` attribute via the
:code:`model` parameter in the endpoint request (see :doc:`rest_api`).

Contributing new metrics clients
--------------------------------

*Coming Soon...*

Testing
-------

*Coming Soon...*

Contributing Documentation
--------------------------

*Coming Soon...*

License
-------

By contributing your code, you agree to license your contribution under the
terms of the APLv2_.

.. _APLv2: http://www.apache.org/licenses/LICENSE-2.0

