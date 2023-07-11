""" path adjuster tests """

# import: standard
import json
import pathlib
import unittest

# import: datax in-house
from datax.utils.deployment_helper.converter.path_adjuster import find_conf_path
from datax.utils.deployment_helper.converter.path_adjuster import get_pipeline_conf_files
from datax.utils.deployment_helper.converter.path_adjuster import read_conf_all
from datax.utils.deployment_helper.converter.path_adjuster import (
    recursive_read_pipeline_conf,
)
from datax.utils.deployment_helper.converter.path_adjuster import replace_conf_reference

# import: external
import git
import pytest
import yaml


class TestFindConfPath(unittest.TestCase):
    """Test Class for testing find_conf_path.

    Class for testing find_conf_path.

    Args:
        unittest.TestCase: An unittest TestCase.

    """

    def test_find_conf_path(self) -> None:
        """Test find conf path using __file__."""
        find_resources_conf = find_conf_path(__file__, src_dir_name="datax")
        git_repo = git.Repo(__file__, search_parent_directories=True)
        git_repo = git_repo.git.rev_parse("--show-toplevel")

        self.assertEqual(
            find_resources_conf, f"{git_repo}/tests/resources/test_path_adjuster/conf"
        )

    def test_find_conf_path_not_found(self) -> None:
        """Test find conf path for error, FileNotFoundError."""
        test_error_dict = {"file": __file__}
        self.assertRaises(FileNotFoundError, find_conf_path, **test_error_dict)


class TestReplaceConfReference(unittest.TestCase):
    """Test Class for testing replace_conf_reference.

    Class for testing replace_conf_reference.

    Args:
        unittest.TestCase: An unittest TestCase.

    """

    def test_replace_conf_reference(self) -> None:
        """Test replacing conf reference."""

        conf_path = "/test/for_test"

        mock_dict = {
            "key_1": "smth",
            "key_2": {
                "sub_k1": "nothing",
                "sub_k2": "conf:/dir/file.txt",
                "sub_k3": None,
            },
            "key_3": "conf:/dir/sub_dir/file.json",
            "key_4": [1, 2],
            "key_5": 123,
        }

        expected_dict = {
            "key_1": "smth",
            "key_2": {
                "sub_k1": "nothing",
                "sub_k2": f"{conf_path}/dir/file.txt",
                "sub_k3": None,
            },
            "key_3": f"{conf_path}/dir/sub_dir/file.json",
            "key_4": [1, 2],
            "key_5": 123,
        }

        replace_conf_reference(mock_dict, conf_path)

        self.assertEqual(mock_dict, expected_dict)


class TestGetPipelineConfFiles(unittest.TestCase):
    """Test Class for testing get_pipeline_conf_files.

    Class for testing get_pipeline_conf_files.

    Args:
        unittest.TestCase: An unittest TestCase.

    """

    def test_get_pipeline_conf_files(self) -> None:
        """Test get_pipeline_conf_files."""

        conf_list = get_pipeline_conf_files(
            "tests/resources/test_path_adjuster", "TestABCModule"
        )

        read_list = [
            "tests/resources/test_path_adjuster/test_pipeline/TestABCModule/app.yml",
            "tests/resources/test_path_adjuster/test_pipeline/TestABCModule/spark.yml",
        ]

        str_list = [str(i) for i in conf_list]

        self.assertEqual(sorted(str_list), sorted(read_list))

    def test_ValueError(self) -> None:
        """Test function for testing get_pipeline_conf_files.

        To test ValueError if found more than 1 config

        """
        self.assertRaises(
            ValueError,
            get_pipeline_conf_files,
            "tests/resources/test_path_adjuster",
            "TestABCModule2",
        )

    def test_FileNotFoundError(self) -> None:
        """Test function for testing get_pipeline_conf_files.

        To test FileNotFoundError if could not find any config

        """
        self.assertRaises(
            FileNotFoundError,
            get_pipeline_conf_files,
            "tests/resources/test_path_adjuster",
            "TestABCModule3",
        )


class TestReadConfAll(unittest.TestCase):
    """Test Class for testing read_conf_all.

    Class for testing read_conf_all.

    Args:
        unittest.TestCase: An unittest TestCase.

    """

    def test_get_read_conf_all(self) -> None:
        """Test read_conf_all."""

        app_path = "tests/resources/test_path_adjuster/conf/app.yml"
        spark_path = "tests/resources/test_path_adjuster/conf/spark.yml"
        logger_path = "tests/resources/test_path_adjuster/conf/logger.json"

        test_paths = [app_path, spark_path, logger_path]
        conf_result = read_conf_all(test_paths)

        conf_app_txt = pathlib.Path(app_path).read_text()
        expected_app = yaml.safe_load(conf_app_txt)
        conf_spark_txt = pathlib.Path(spark_path).read_text()
        expected_spark = yaml.safe_load(conf_spark_txt)
        conf_logger_txt = pathlib.Path(logger_path).read_text()
        expected_logger = json.loads(conf_logger_txt)

        self.assertEqual(conf_result["app"], expected_app)
        self.assertEqual(conf_result["spark"], expected_spark)
        self.assertEqual(conf_result["logger"], expected_logger)


def test_recursive_read_pipeline_conf():
    """
    Test recursive_read_pipeline_conf function by reading the prepared config files
    and compare the result with the expected one.
    """
    git_repo = git.Repo(__file__, search_parent_directories=True)
    git_repo = git_repo.git.rev_parse("--show-toplevel")
    test_conf_path = f"{git_repo}/tests/resources/test_path_adjuster"
    test_conf_module = "TestRecursive"

    result_conf_dict = recursive_read_pipeline_conf(test_conf_path, test_conf_module)

    expected_output = {
        "app": {"TestRecursive": {"key": "value"}, "AnotherSection": {"key": "value"}},
        "audit": {"test": {"key": "value"}, "deequ": {"check": None}},
    }
    assert result_conf_dict == expected_output


@pytest.mark.parametrize(
    "parent_dir_name, expected_output",
    [
        (
            None,
            {
                "app": {
                    "TestRecursive": {"key": "value"},
                    "AnotherSection": {"key": "value"},
                },
                "audit": {"test": {"key": "value"}, "deequ": {"check": None}},
                "data_profiling": {
                    "deequ": {"example_analyzer": {"Analyzer": {"key": "value"}}}
                },
            },
        ),
        (
            "workflow",
            {
                "data_profiling": {
                    "deequ": {"example_analyzer": {"Analyzer": {"key": "value"}}}
                },
            },
        ),
    ],
)
def test_recursive_read_pipeline_conf_parent_dir_name(parent_dir_name, expected_output):
    """
    Test recursive_read_pipeline_conf function by reading the prepared config files
    with specified parent_dir_name.

    Args:
        parent_dir_name (str): Input parent directory name.
        expected_output (dict): The expected return config files dictionary.

    Assertion statement:
        1. Validate if the function return config files correctly with provided
            parent_dir_name for each case.
    """
    git_repo = git.Repo(__file__, search_parent_directories=True)
    git_repo = git_repo.git.rev_parse("--show-toplevel")
    test_conf_path = f"{git_repo}/tests/resources/test_path_adjuster"
    test_conf_module = "TestRecursive"

    result_conf_dict = recursive_read_pipeline_conf(
        test_conf_path, test_conf_module, parent_dir_name
    )

    assert (
        result_conf_dict == expected_output
    ), f"The result {result_conf_dict} did not match the expected {expected_output}"
