"""test of common entry-point handler utility modules"""

# import: datax in-house
from datax.utils.deployment_helper.entrypoint.common import get_pipeline_args


class MockABCPipeline:
    """Test class for testing `get_pipeline_args` function"""

    def __init__(self, data_source: str) -> None:
        """__init__ function to initiate class instance"""
        self.table = data_source

    def execcute(self) -> None:
        """pipeline execution function"""
        pass


def test_get_pipeline_args() -> None:
    """Test the `get_pipeline_args` function.

    To validate if the function get and filter the class arguments correctly.

    Assertion statement:
        1. Validate output pipeline arguments match the expected arguments
            passed to the __init__ function based on the class.
    """
    test_input_args = {
        "data_source": "example_table",
        "start_date": "2022-02-02",
        "end_date": "2022-02-03",
    }

    pipeline_args = get_pipeline_args(pipeline_cls=MockABCPipeline, args=test_input_args)
    expected = {"data_source": "example_table"}

    assert (
        pipeline_args == expected
    ), f"The extracted pipeline arguments {pipeline_args} did not match the expected {expected}."
