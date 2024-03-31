__all__ = ["ConfigLoader"]


import argparse
import json
from typing import TypeVar, Union, Generic

T = TypeVar("T", bound="NestedDict")


class NestedDict(
    dict[str, Union[str, dict[str, Union[str, "NestedDict"]]]], Generic[T]
):
    pass


def load_config() -> NestedDict:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config")
    args = parser.parse_args()

    with open(args.config, "r") as f:
        config = json.load(f)
    return config


class ConfigLoader(dict):
    """
    ConfigLoader is a subclass of the built-in Python dictionary (dict) class. It is designed to load and store configuration parameters.

    The constructor of this class calls the `load_config` function, which reads a configuration file and returns a dictionary. This dictionary is then used to initialize the ConfigLoader instance.

    By subclassing dict, ConfigLoader inherits all the methods of a dictionary, and can be used wherever a dictionary would be used. This includes indexing, iteration, and membership tests using 'in'.

    Additional methods or attributes can be added to this class if there are specific behaviors you want for your configuration loading.
    """
    def __init__(self) -> None:
        super().__init__(load_config())
