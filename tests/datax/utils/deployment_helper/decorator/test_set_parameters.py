# import: standard
import unittest

# import: pyspark
from pyspark.sql import SparkSession

# import: datax in-house
from datax.utils.deployment_helper.decorator.set_parameters import _default_obj
from datax.utils.deployment_helper.decorator.set_parameters import _pipeline_obj
from datax.utils.deployment_helper.decorator.set_parameters import set_default_obj
from datax.utils.deployment_helper.decorator.set_parameters import set_pipeline_obj
from datax.utils.deployment_helper.decorator.set_parameters import set_tfm_obj


class Test_Set_Default_Obj(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.conf = {"key1": "value1", "key2": "value2"}
        self.spark = SparkSession.builder.getOrCreate()
        self.logger = self.spark._jvm.org.apache.log4j.LogManager.getLogger(
            "test_set_default_obj"
        )
        self.dbutils = None

    @set_default_obj
    def test(self):
        self.assertIsInstance(_default_obj["spark"], SparkSession)
        self.assertEqual(_default_obj["default"]["conf"], self.conf)
        self.assertTrue(_default_obj["from_handler"], "from_handler is not True")
        try:
            _default_obj["default"]["logger"].warn("Test logger")
        except KeyError:
            self.fail("logger raised KeyError unexpectedly!")


class Test_Set_Pipeline_Obj(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        conf = {
            "Test_Set_Pipeline_Obj": {
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
        logger = spark._jvm.org.apache.log4j.LogManager.getLogger("test_set_pipeline_obj")
        dbutils = None
        _default_obj["spark"] = spark
        _default_obj["default"]["conf"] = conf
        _default_obj["default"]["logger"] = logger
        _default_obj["default"]["dbutils"] = dbutils
        _default_obj["from_handler"] = True

    @set_pipeline_obj
    def test(self, spark, conf=None, logger=None, dbutils=None):
        self.assertIsInstance(spark, SparkSession)
        self.assertEqual(conf, self.test_conf["Test_Set_Pipeline_Obj"])
        self.assertTrue(_default_obj["from_pipeline"], "from_pipeline is not True")
        self.assertEqual(self.key1, "value1")
        self.assertEqual(self.key2, "value2")
        try:
            logger.warn("Test logger")
        except KeyError:
            self.fail("logger raised KeyError unexpectedly!")


class Test_Set_Tfm_Obj(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        conf = {
            "Test_Set_Tfm_Obj": {
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
        logger = spark._jvm.org.apache.log4j.LogManager.getLogger("test_set_tfm_obj")
        dbutils = None
        _pipeline_obj["spark"] = spark
        _pipeline_obj["default"]["conf"] = conf
        _pipeline_obj["default"]["logger"] = logger
        _pipeline_obj["default"]["dbutils"] = dbutils

        _default_obj["from_pipeline"] = True

    @set_tfm_obj
    def test(self, key1, key2, data_source, spark, logger=None, dbutils=None):
        self.assertIsInstance(spark, SparkSession)
        self.assertEqual(key1, "value1")
        self.assertEqual(key2, "value2")
        self.assertEqual(data_source, self.test_conf["Test_Set_Tfm_Obj"]["data_source"])
        try:
            logger.warn("Test logger")
        except KeyError:
            self.fail("logger raised KeyError unexpectedly!")
