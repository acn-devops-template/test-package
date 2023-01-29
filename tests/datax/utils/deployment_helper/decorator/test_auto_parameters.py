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

        Set conf_all, spark, logger, dbutils to self before test function.

        """
        self.conf_app = {"name": "test_init_auto_parameters"}
        self.conf_spark = {"spark.app.name": "test_init_auto_parameters"}
        self.conf_logger = {}
        self.conf_test = {"key": "value"}
        self.conf_all = {
            "app": self.conf_app,
            "spark": self.conf_spark,
            "logger": self.conf_logger,
            "test": self.conf_test,
        }
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

        self.assertIsInstance(_default_obj["default"]["spark"], SparkSession)
        self.assertEqual(_default_obj["default"]["conf_app"], self.conf_app)
        self.assertEqual(_default_obj["default"]["conf_spark"], self.conf_spark)
        self.assertEqual(_default_obj["default"]["conf_logger"], {})
        self.assertEqual(_default_obj["default"]["conf_test"], self.conf_all["test"])
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

        self.conf_app = {
            "Test_parse_auto_parameters": {
                "key1": "value1",
                "key2": "value2",
                "data_processor_name": "str",
                "main_transformation_name": "str",
                "output_data_path": "str",
                "output_schema_path": "path/to/schema.json",
            }
        }
        self.conf_deequ = {"deequ": {"key1": "value1", "key2": "value2"}}

        self.spark = SparkSession.builder.getOrCreate()
        self.logger = self.spark._jvm.org.apache.log4j.LogManager.getLogger(
            "test_parse_auto_parameters"
        )
        _default_obj["default"] = {
            "spark": self.spark,
            "conf_app": self.conf_app,
            "conf_deequ": self.conf_deequ,
            "conf_logger": {},
            "logger": self.logger,
            "dbutils": "test_dbutils",
        }
        _default_obj["from_handler"] = True

    @parse_auto_parameters
    def test(
        self,
        spark: SparkSession,
        conf_app: Dict,
        conf_deequ: Dict,
        conf_logger: Dict,
        logger: Any = None,
        dbutils: Any = None,
    ) -> None:
        """Test function for testing parse_auto_parameters.

        Main test function of Test_parse_auto_parameters.

        Args:
            spark (SparkSession): A SparkSession.
            conf_app (Dict): conf_app dict.
            conf_deequ (Dict): conf_deequ dict.
            conf_logger (Dict): conf_logger dict:
            logger: Log4j logger
            dbutils: DBUtils

        """
        self.assertIsInstance(spark, SparkSession)
        self.assertEqual(dbutils, "test_dbutils")
        self.assertTrue(_default_obj["from_pipeline"], "from_pipeline is not True")

        self.assertEqual(conf_app, self.conf_app)
        self.assertEqual(conf_deequ, self.conf_deequ)
        self.assertEqual(conf_logger, {})

        try:
            logger.warn("Test logger")
        except KeyError:
            self.fail("logger raised KeyError unexpectedly!")

    def test_manual_run_pipeline(self):
        """Test function for testing parse_auto_parameters manually."""

        class DummyPipeline:
            """Mockup Pipeline Class"""

            @parse_auto_parameters
            def __init__(
                cls,
                spark: SparkSession,
                conf_app: Dict,
                conf_deequ: Dict,
                logger: Any = None,
                dbutils: Any = None,
            ) -> None:
                """Init function for DummyPipeline"""
                cls.spark = spark
                cls.conf_app = conf_app
                cls.conf_deequ = conf_deequ
                cls.logger = logger
                cls.dbutils = dbutils

            def main(cls):
                """Prepare data function inside ETL class, return dataframe"""
                return cls.spark, cls.dbutils, cls.conf_app, cls.conf_deequ, cls.logger

        _default_obj["from_handler"] = False

        spark, dbutils, conf_app, conf_deequ, logger = DummyPipeline(
            self.spark, self.conf_app, self.conf_deequ, self.logger, "test_dbutils_manual"
        ).main()

        self.assertIsInstance(spark, SparkSession)
        self.assertEqual(dbutils, "test_dbutils_manual")
        self.assertEqual(conf_app, self.conf_app)
        self.assertEqual(conf_deequ, self.conf_deequ)

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

        conf_app = {
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
        self.conf_spark = {"spark": {"spark.sql.shuffle.partitions": "2"}}
        self.test_conf = conf_app
        spark = SparkSession.builder.getOrCreate()
        logger = spark._jvm.org.apache.log4j.LogManager.getLogger(
            "test_get_auto_parameters"
        )

        _pipeline_obj["default"] = {
            "spark": spark,
            "conf_app": conf_app,
            "conf_spark": self.conf_spark,
            "logger": logger,
            "dbutils": "dbutils",
        }
        _default_obj["from_pipeline"] = True

    @get_auto_parameters
    def test(
        self,
        key1: str,
        key2: str,
        data_source: Dict,
        spark: SparkSession,
        conf_spark: Dict,
        conf_app: Dict,
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
        self.assertEqual(conf_spark, self.conf_spark)
        self.assertEqual(conf_app, self.test_conf)
        self.assertEqual(dbutils, "dbutils")
        try:
            logger.warn("Test logger")
        except KeyError:
            self.fail("logger raised KeyError unexpectedly!")
