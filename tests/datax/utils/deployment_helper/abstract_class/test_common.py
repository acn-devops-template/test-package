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

        Return:
            SparkSession: spark
            Dict: conf_app dict
            Dict: conf_spark dict
            Dict: conf_logger dict
            Dict: conf_deequ dict
            Dict: conf_all dict

        """
        return (
            self.spark,
            self.conf_app,
            self.conf_spark,
            self.conf_logger,
            self.conf_deequ,
            self.conf_all,
        )


def test() -> None:
    """Test function for testing Task(ABC).

    To test spark and conf value of Task(ABC)

    """
    task = Mock_ABC(
        module_name="TestABCModule", conf_path="./tests/resources/test_common"
    )
    (
        test_spark,
        test_conf_app,
        test_conf_spark,
        test_conf_logger,
        test_conf_deequ,
        test_conf_all,
    ) = task.launch()

    confValue = yaml.safe_load(
        pathlib.Path(
            "./tests/resources/test_common/test_pipeline/TestABCModule/app.yml"
        ).read_text()
    )

    sparkconfValue = yaml.safe_load(
        pathlib.Path(
            "./tests/resources/test_common/test_pipeline/TestABCModule/spark.yml"
        ).read_text()
    )

    assert confValue == test_conf_app
    assert sparkconfValue == test_conf_spark
    assert test_conf_logger == {}
    assert test_conf_deequ == {}
    assert test_conf_all["app"] == confValue
    assert test_conf_all["spark"] == sparkconfValue
    assert test_conf_all["logger"] == {}
    assert test_conf_all["deequ"] == {}
    assert type(test_spark) == SparkSession


def test_wo_pipeline_section() -> None:
    """Test function for testing Task(ABC).

    To test if there is no pipeline section in conf_app

    """
    task = Mock_ABC(
        module_name="TestABCModuleWO", conf_path="./tests/resources/test_common"
    )
    (
        test_spark,
        test_conf_app,
        test_conf_spark,
        test_conf_logger,
        test_conf_deequ,
        test_conf_all,
    ) = task.launch()

    confValue = yaml.safe_load(
        pathlib.Path(
            "./tests/resources/test_common/test_pipeline/TestABCModuleWO/app.yml"
        ).read_text()
    )
    print(test_conf_all)

    assert confValue == test_conf_app
    assert test_conf_all["app"] == confValue
    assert type(test_spark) == SparkSession
    assert test_conf_all["spark"] == {}
    assert test_conf_all["logger"] == {}
    assert test_conf_all["deequ"] == {}


def test_ValueError() -> None:
    """Test function for testing Task(ABC).

    To test ValueError if found more than 1 config

    """
    with pytest.raises(ValueError):
        Mock_ABC(module_name="TestABCModule2", conf_path="./tests/resources/test_common")


def test_FileNotFoundError() -> None:
    """Test function for testing Task(ABC).

    To test FileNotFoundError if could not find any config

    """
    with pytest.raises(FileNotFoundError):
        Mock_ABC(module_name="TestABCModule3", conf_path="./tests/resources/test_common")
