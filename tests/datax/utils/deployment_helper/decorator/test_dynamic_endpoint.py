""" decorator test of register_dynamic_endpoint """

# import: standard
import argparse
import sys
from typing import Any
from typing import Callable
from typing import Dict
from typing import TypeVar

# import: datax in-house
from datax.utils.deployment_helper.decorator.dynamic_endpoint import _dynamic_endpoint
from datax.utils.deployment_helper.decorator.dynamic_endpoint import (
    register_dynamic_endpoint,
)


@register_dynamic_endpoint
def register_dynamic_endpoint_deco(x: int, y: int) -> int:
    """Mock function for testing register_dynamic_endpoint

    Args:
        x (int): Integer to sum
        y (int): Integer to sum

    Returns:
        int: x + y.

    """

    return x + y


def dynamic_endpoint_execute(
    obj: Dict[str, Callable], module_name: str, arg_a: int, arg_b: int
) -> Any:
    """Function for selecting key of obj dict using module_name.

    Receive dict and module_name and return dict[module_name].

    Args:
        obj (Dict[str, Callable]): _dynamic_endpoint dict.
        module_name (str): Module name.
        arg_a (int): Integer to sum.
        arg_b (int): Integer to sum.

    Returns:
        int: Result of a module ('register_dynamic_endpoint_deco').

    """

    return obj[module_name](arg_a, arg_b)


def test_dynamic_endpoint_main():
    """Function for testing register_dynamic_endpoint.

    Main test function for register_dynamic_endpoint.

    """

    parser = argparse.ArgumentParser(description="Test endpoint dynamic registery")
    parser.add_argument(
        "--module", help="test module name", default="register_dynamic_endpoint_deco"
    )
    parser.add_argument("--x", help="a random numerical", default=7)
    parser.add_argument("--y", help="a random numerical", default=7)
    args = parser.parse_known_args(sys.argv[1:])[0]

    result_test = dynamic_endpoint_execute(_dynamic_endpoint, args.module, args.x, args.y)
    result_ref = register_dynamic_endpoint_deco(args.x, args.y)

    assert result_test == result_ref
