# import: pyspark
from pyspark.sql import SparkSession

# import: external
import pytest

# Configurations for Spark Session can be specified here.
SPARK_CONFIGS = {
    "master": "local[*]",
    "app_name": "Unit Test App",
    "other_configs": {},
}


@pytest.fixture(scope="session")
def spark(request):
    """
    Custom fixture for instantiating a spark session.
    This spark session is used across test cases running under the same test session.
    After all the test cases finished, the session will be stopped automatically.
    """

    master = SPARK_CONFIGS["master"]
    app_name = SPARK_CONFIGS["app_name"]
    other_configs = SPARK_CONFIGS["other_configs"]

    spark_builder = SparkSession.builder.master(master).appName(app_name)

    # add additional configs
    for key, val in other_configs.items():
        spark_builder.config(key, val)

    # create a spark session
    spark_session = spark_builder.getOrCreate()

    # stop spark session after all tests finished
    request.addfinalizer(lambda: spark_session.sparkContext.stop())

    yield spark_session
