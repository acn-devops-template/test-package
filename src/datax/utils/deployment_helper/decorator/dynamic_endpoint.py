"""decorator dynamic_endpoint module"""

# import: standard
from typing import Callable

# dynamic module parser
_dynamic_endpoint = {}


def register_dynamic_endpoint(module: Callable) -> Callable:
    """Decorator function for dynamic endpoint.

    Register a module in '_dynamic_endpoint' dict.

    Args:
        module (Callable): A class or function to store.

    Returns:
        Callable: A stored class or function.

    """
    _dynamic_endpoint[module.__name__] = module

    return module
