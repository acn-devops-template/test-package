"""decorator integration test of set_default_obj, set_pipeline_obj, set_tfm_obj modules"""

# import: standard
import unittest

# import: pyspark
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession

# import: datax in-house
from datax.utils.deployment_helper.decorator.set_parameters import set_default_obj
from datax.utils.deployment_helper.decorator.set_parameters import set_pipeline_obj
from datax.utils.deployment_helper.decorator.set_parameters import set_tfm_obj


class Test_Integration_Set_Parameters(unittest.TestCase):
    """Test Class for integration test of set_parameters.

    Args:
        unittest.TestCase: An unittest TestCase.

    """

    @classmethod
    def setUpClass(self):
        """Test Function for setting up self vars.

        Set conf, spark, logger, dbutils to self before test function

        """
        self.conf = {
            "_Pipeline": {
                "key1": "value_1",
                "key2": "value_2",
                "data_processor_name": "str",
                "main_transformation_name": "str",
                "output_data_path": "str",
                "output_schema_path": "path/to/schema.json",
            },
            "_Agg": {
                "key1": "value1",
                "key2": "value2",
                "data_source": {
                    "input_data_endpoint": "tests/resources/people.json",
                    "input_schema_path": "tests/resources/people_schema.json",
                    "ref_schema_path": "tests/resources/people_schema.json",
                },
            },
        }
        self.spark = SparkSession.builder.getOrCreate()
        self.logger = self.spark._jvm.org.apache.log4j.LogManager.getLogger(
            "test_set_parameters"
        )
        self.dbutils = None

    @set_default_obj
    def test(self):
        """Test function for set_parameters.

        Main test function of Test_Integration_Set_Parameters.

        """
        self.logger.info("Launching task")
        pipeline_obj = _Pipeline("2022-01-01", "2022-01-01")
        ret = pipeline_obj.execute()

        self.assertIsInstance(ret, DataFrame)
        self.assertEqual(ret.count(), 1)
        self.assertEqual(ret.select("age").collect()[0]["age"], 30)
        self.assertEqual(ret.select("address").collect()[0]["address"], "Lisbon")
        self.assertEqual(
            ret.select("key1").collect()[0]["key1"], self.conf["_Agg"]["key1"]
        )
        self.assertEqual(
            ret.select("key2").collect()[0]["key2"], self.conf["_Agg"]["key2"]
        )


class _Pipeline:
    """
    _Pipeline mock class for integration test of set_parameters

    """

    @set_pipeline_obj
    def __init__(
        self,
        start_date,
        end_date,
        spark,
        conf=None,
        logger=None,
        dbutils=None,
    ):
        """Mock __init__ of _Pipeline for set_pipeline_obj.

        Args:
            start_date: start_date
            end_date: end_date
            spark (SparkSession): A SparkSession.
            conf (Dict): conf dict.
            logger: Log4j logger
            dbutils: DBUtils

        """
        self.start_date = start_date
        self.end_date = end_date

        # optional
        self.spark = spark
        self.logger = logger

    def execute(self):
        """Main Test function of _Pipeline.

        Execute _Agg prepare_data function.

        Returns:
            Dataframe

        """
        mul_src_obj = _Agg(
            self.start_date,
            self.end_date,
        )
        self.logger.info(f"{self.key1}")
        self.logger.info(f"{self.key2}")
        df = mul_src_obj.prepare_data()
        return df


class _Agg:
    """
    _Agg mock class for integration test of set_parameters

    """

    @set_tfm_obj
    def __init__(
        self,
        start_date,
        end_date,
        key1,
        key2,
        data_source,
        spark=None,
        logger=None,
        dbutils=None,
    ):
        """Mock __init__ of _Agg for set_tfm_obj.

        Args:
            start_date: start_date
            end_date: end_date
            key1: test var 1
            key2: test var 2
            data_source (Dict): data_source dict
            spark (SparkSession): A SparkSession.
            logger: Log4j logger
            dbutils: DBUtils

        """
        self.spark = spark
        self.data_source = data_source
        self.logger = logger

        self.start_date = start_date
        self.end_date = end_date

        self.key1 = key1
        self.key2 = key2

    def load_source(self):
        """Test function of _Agg for loading data source.

        Load input_data_endpoint from 'data_source'.

        Returns:
            Dataframe

        """

        df = self.spark.read.json(self.data_source["input_data_endpoint"])

        selected_cols = [
            "age",
            "address",
        ]

        df = df.select(*selected_cols)

        return df

    def prepare_data(self):
        """Test function of _Agg for preparing dataframe.

        Main function of _Agg.
        Load data source and use conf to add columns.

        Returns:
            Dataframe

        """

        self.logger.warn(self.spark.sparkContext._conf.get("spark.app.name"))

        df = self.load_source()
        df = df.withColumn("key1", F.lit(self.key1)).withColumn("key2", F.lit(self.key2))

        return df