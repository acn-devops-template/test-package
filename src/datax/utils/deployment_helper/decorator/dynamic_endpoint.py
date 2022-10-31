# import: standard
from typing import Callable

# dynamic module parser
_dynamic_endpoint = {}


def register_dynamic_endpoint(module: Callable) -> Callable:

    _dynamic_endpoint[module.__name__] = module

    return module
