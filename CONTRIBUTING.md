# Contributing to Caladrius

Contributions to Caladrius are very welcome. The systems was designed to be
extensible and the aim of the project is to provide a comprehensive suite of
performance models to suit a wide range of situations.

## Contributing new model

New models are easy to add to Caladrius. The model classes used by each
endpoint are defined in the `yaml` config file passed to the `app.py` script.
An example configuration showing how to specify the model classes used is shown
in `config/main.yaml.example`.

The config file uses absolute import paths for each class. Therefore, provided
that your source files are reachable on the `PYTHONPATH` (with `__init__.py` in
required subdirectories) your model does not need to be within the Caladrius
root folder. However, you will need to add the folder above the Caladrius repo
to your `PYTHONPATH` to access the abstract base classes and other utility
features therein.

## Testing

_Coming Soon..._

## Contributing Documentation

_Coming Soon..._

## License

By contributing your code, you agree to license your contribution under the
terms of the [APLv2](LICENSE).
