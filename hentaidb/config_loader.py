import argparse
import json
from typing import TypeVar, Union, Generic

T = TypeVar('T', bound='NestedDict')

class NestedDict(dict[str, Union[str, dict[str, Union[str, 'NestedDict']]]], Generic[T]):
    pass

def load_config() -> NestedDict:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config")
    args = parser.parse_args()

    with open(args.config, "r") as f:
        config = json.load(f)
    return config