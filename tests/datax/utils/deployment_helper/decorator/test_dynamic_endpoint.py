# import: standard
import argparse
from typing import Any
from typing import Callable
from typing import Dict

# import: internal
from src.datax.utils.deployment_helper.decorator.dynamic_endpoint import _dynamic_endpoint
from src.datax.utils.deployment_helper.decorator.dynamic_endpoint import (
    register_dynamic_endpoint,
)


@register_dynamic_endpoint
def test_register_dynamic_endpoint_deco(x: int, y: int) -> int:

    return x + y


def test_dynamic_endpoint_execute(
    obj: Dict[Callable], module_name: str, arg_a: int, arg_b: int
) -> Any:

    return obj[module_name](arg_a, arg_b)


def test_dynamic_endpoint_main() -> bool:

    parser = argparse.ArgumentParser(description="Test endpoint dynamic registery")
    parser.add_argument(
        "--module", help="test module name", default="test_dynamic_endpoint_execute"
    )
    parser.add_argument("--x", help="a random numerical", default=7)
    parser.add_argument("--y", help="a random numerical", default=7)
    args = parser.parse_args()

    result_test = test_dynamic_endpoint_execute(
        _dynamic_endpoint, args.module, args.x, args.y
    )
    result_ref = test_register_dynamic_endpoint_deco(args.x, args.y)

    return result_test == result_ref
