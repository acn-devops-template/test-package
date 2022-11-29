"""decorator test of init_auto_parameters, parse_auto_parameters, get_auto_parameters modules"""

# import: standard
import unittest
from typing import Any
from typing import Dict

# import: pyspark
from pyspark.sql import SparkSession

# import: datax in-house
from datax.utils.deployment_helper.decorator.auto_parameters import _default_obj
from datax.utils.deployment_helper.decorator.auto_parameters import _pipeline_obj
from datax.utils.deployment_helper.decorator.auto_parameters import get_auto_parameters
from datax.utils.deployment_helper.decorator.auto_parameters import init_auto_parameters
from datax.utils.deployment_helper.decorator.auto_parameters import parse_auto_parameters


class Test_init_auto_parameters(unittest.TestCase):
    """Test Class for testing init_auto_parameters.

    Class for testing init_auto_parameters.

    Args:
        unittest.TestCase: An unittest TestCase.

    """

    conf: Dict
    spark: SparkSession
    logger: Any
    dbutils: Any

    @classmethod
    def setUpClass(self) -> None:
        """Setup function for testing init_auto_parameters.

        Set conf, spark, logger, dbutils to self before test function.

        """
        self.conf = {"key1": "value1", "key2": "value2"}
        self.spark = SparkSession.builder.getOrCreate()
        self.logger = self.spark._jvm.org.apache.log4j.LogManager.getLogger(
            "test_init_auto_parameters"
        )
        self.dbutils = None

    @init_auto_parameters
    def test(self) -> None:
        """Test function for testing init_auto_parameters.

        Main test function of Test_init_auto_parameters.

        """

        self.assertIsInstance(_default_obj["spark"], SparkSession)
        self.assertEqual(_default_obj["default"]["conf"], self.conf)
        self.assertTrue(_default_obj["from_handler"], "from_handler is not True")
        try:
            _default_obj["default"]["logger"].warn("Test logger")
        except KeyError:
            self.fail("logger raised KeyError unexpectedly!")


class Test_parse_auto_parameters(unittest.TestCase):
    """Test Class for testing parse_auto_parameters.

    Class for testing parse_auto_parameters.

    Args:
        unittest.TestCase: An unittest TestCase.

    """

    test_conf: Dict

    @classmethod
    def setUpClass(self) -> None:
        """Setup function for testing parse_auto_parameters.

        Set conf, spark, logger, dbutils to _default_obj before test function

        """
        conf = {
            "Test_parse_auto_parameters": {
                "key1": "value1",
                "key2": "value2",
                "data_processor_name": "str",
                "main_transformation_name": "str",
                "output_data_path": "str",
                "output_schema_path": "path/to/schema.json",
            }
        }
        self.test_conf = conf
        spark = SparkSession.builder.getOrCreate()
        logger = spark._jvm.org.apache.log4j.LogManager.getLogger(
            "test_parse_auto_parameters"
        )
        dbutils = None
        _default_obj["spark"] = spark
        _default_obj["default"]["conf"] = conf
        _default_obj["default"]["logger"] = logger
        _default_obj["default"]["dbutils"] = dbutils
        _default_obj["from_handler"] = True

    @parse_auto_parameters
    def test(
        self,
        spark: SparkSession,
        conf: Dict = None,
        logger: Any = None,
        dbutils: Any = None,
    ) -> None:
        """Test function for testing parse_auto_parameters.

        Main test function of Test_parse_auto_parameters.

        Args:
            spark (SparkSession): A SparkSession.
            conf (Dict): conf dict.
            logger: Log4j logger
            dbutils: DBUtils

        """
        self.assertIsInstance(spark, SparkSession)
        self.assertEqual(conf, self.test_conf)
        self.assertTrue(_default_obj["from_pipeline"], "from_pipeline is not True")
        self.assertEqual(conf["Test_parse_auto_parameters"]["key1"], "value1")
        self.assertEqual(conf["Test_parse_auto_parameters"]["key2"], "value2")
        try:
            logger.warn("Test logger")
        except KeyError:
            self.fail("logger raised KeyError unexpectedly!")


class Test_get_auto_parameters(unittest.TestCase):
    """Test Class for testing get_auto_parameters.

    Class for testing get_auto_parameters.

    Args:
        unittest.TestCase: An unittest TestCase.

    """

    test_conf: Dict

    @classmethod
    def setUpClass(self) -> None:
        """Setup function for testing get_auto_parameters.

        Set conf, spark, logger, dbutils to _pipeline_obj before test function

        """

        conf = {
            "Test_get_auto_parameters": {
                "key1": "value1",
                "key2": "value2",
                "data_source": {
                    "input_data_endpoint": "resources/people.json",
                    "input_schema_path": "resources/people_schema.json",
                    "ref_schema_path": "resources/people_schema.json",
                },
            }
        }
        self.test_conf = conf
        spark = SparkSession.builder.getOrCreate()
        logger = spark._jvm.org.apache.log4j.LogManager.getLogger(
            "test_get_auto_parameters"
        )
        dbutils = None
        _pipeline_obj["spark"] = spark
        _pipeline_obj["default"]["conf"] = conf
        _pipeline_obj["default"]["logger"] = logger
        _pipeline_obj["default"]["dbutils"] = dbutils

        _default_obj["from_pipeline"] = True

    @get_auto_parameters
    def test(
        self,
        key1: str,
        key2: str,
        data_source: Dict,
        spark: SparkSession,
        logger: Any = None,
        dbutils: Any = None,
    ) -> None:
        """Test function for testing get_auto_parameters.

        Main test function of Test_get_auto_parameters.

        Args:
            key1: test variable 1
            key2: test variable 2
            data_source (Dict): data_source dict
            spark (SparkSession): A SparkSession.
            logger: Log4j logger
            dbutils: DBUtils

        """
        self.assertIsInstance(spark, SparkSession)
        self.assertEqual(key1, "value1")
        self.assertEqual(key2, "value2")
        self.assertEqual(
            data_source, self.test_conf["Test_get_auto_parameters"]["data_source"]
        )
        try:
            logger.warn("Test logger")
        except KeyError:
            self.fail("logger raised KeyError unexpectedly!")
