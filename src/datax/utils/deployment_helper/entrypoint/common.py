"""common entry-point handler utility modules"""

# import: standard
import inspect
from typing import Callable


def get_pipeline_args(pipeline_cls: Callable, args: dict) -> dict:
    """Extract the signature (input arguments) of the pipeline's __init__ method.

    This method is used to handle different dynamic arguments based on the pipeline class in the endpoint.

    Args:
        pipeline_cls (callable): The callable pipeline class
        args (dict): Input CLI arguments

    Returns:
        dict: The pipeline arguments

    """
    signature = inspect.signature(pipeline_cls.__init__)  # type: ignore
    pipeline_args = {
        key: value for key, value in args.items() if key in signature.parameters
    }

    return pipeline_args
