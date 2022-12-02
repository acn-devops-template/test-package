"""abstract_class test of common modules"""

# import: standard
import pathlib
from typing import Dict
from typing import Tuple

# import: pyspark
from pyspark.sql import SparkSession

# import: datax in-house
from datax.utils.deployment_helper.abstract_class.common import Task

# import: external
import pytest
import yaml


class Mock_ABC(Task):
    """Test Class for testing Task(ABC).

    To call Task and return spark and conf

    """

    def launch(self) -> Tuple[SparkSession, Dict]:
        """Test function for testing Task(ABC).

        To return spark and conf

        Return:
            SparkSession: spark
            Dict: conf dict

        """
        return self.spark, self.conf


def test() -> None:
    """Test function for testing Task(ABC).

    To test spark and conf value of Task(ABC)

    """
    task = Mock_ABC(module_name="Test_ABC_Module", conf_path="./tests/resources/")
    test_spark, test_conf = task.launch()

    firstValue = yaml.safe_load(
        pathlib.Path(
            "./tests/resources/test_pipeline/test_conf_file/Test_ABC_Module.yml"
        ).read_text()
    )

    assert firstValue == test_conf
    assert type(test_spark) == SparkSession


def test_ValueError() -> None:
    """Test function for testing Task(ABC).

    To test ValueError if found more than 1 config

    """
    with pytest.raises(ValueError):
        Mock_ABC(module_name="Test_ABC_Module_2", conf_path="./tests/resources/")


def test_FileNotFoundError() -> None:
    """Test function for testing Task(ABC).

    To test FileNotFoundError if could not find any config

    """
    with pytest.raises(FileNotFoundError):
        Mock_ABC(module_name="Test_ABC_Module_3", conf_path="./tests/resources/")
