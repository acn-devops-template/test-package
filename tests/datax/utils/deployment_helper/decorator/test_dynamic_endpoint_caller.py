""" decorator test of dynamic_endpoint_caller """

# import: standard
import subprocess


def test_dynamic_endpoint_caller_with_default_arg() -> None:
    """Test function for testing test_dynamic_endpoint.py

    Test function to call test_dynamic_endpoint.py.

    """

    output = subprocess.run(
        [
            "python",
            "./tests/deployment_helper/decorator/test_dynamic_endpoint.py",
            "-m",
            "test_dynamic_endpoint_main",
        ]
    )

    assert output.returncode == 0
