"""abstract_class common module"""

# import: standard
import copy
from abc import ABC
from abc import abstractmethod
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Union

# import: pyspark
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession

# import: datax in-house
from datax.utils.deployment_helper.abstract_class.conf_file_reader import ConfFileReader
from datax.utils.deployment_helper.converter.path_adjuster import get_pipeline_conf_files
from datax.utils.deployment_helper.converter.path_adjuster import replace_conf_reference


def get_dbutils(spark: SparkSession) -> Optional[Any]:
    """Function to get dbutils.

    Get DBUtils from pyspark.dbutils if error return None.
    please note that this function is used in mocking by its name.

    Args:
        spark (SparkSession): An input SparkSession.

    Returns:
        DBUtils(spark) for success, None otherwise.

    """
    try:
        # import: pyspark
        from pyspark.dbutils import DBUtils  # noqa

        if "dbutils" not in locals():
            utils = DBUtils(spark)
            return utils
        else:
            return locals().get("dbutils")
    except ImportError:
        return None


class Task(ABC):
    """Abstract Base Class to initiate a pipeline.

    This is an abstract class that provides handy interfaces to implement workloads (e.g. jobs or job tasks).
    Create a child from this class and implement the abstract launch method.
    Class provides access to the following useful objects:
        * self.spark is a SparkSession
        * self.dbutils provides access to the DBUtils
        * self.logger provides access to the Spark-compatible logger
        * self.conf provides access to the parsed configuration of the job

    """

    def __init__(
        self,
        module_name: str,
        conf_path: Optional[str] = None,
        spark: Optional[SparkSession] = None,
        init_app_conf: Optional[Dict] = None,
        init_spark_conf: Optional[Dict] = None,
        init_logger_conf: Optional[Dict] = None,
    ) -> None:
        """__init__ function of Task class.

        Set following useful objects: self.conf, self.spark, self.logger, self.dbutils.

        Args:
            module_name (str): Use this input as self.module_name.
            conf_path (Optional[str]):  Use this path to find conf files.
            spark (Optional[SparkSession]): Use this input as self.spark if provided, create a SparkSession otherwise.
            init_app_conf (Optional[Dict]): If provided, use this input as self.all_conf["app"].
            init_spark_conf (Optional[Dict]): If provided, use this input as self.all_conf["spark"].
            init_logger_conf (Optional[Dict]): If provided, use this input as self.all_conf["logger"].

        """
        self.module_name = module_name

        if conf_path:
            self.all_conf = self._provide_all_config(conf_path)
        else:
            self.all_conf = {}

        if init_app_conf:
            self.all_conf["app"] = init_app_conf
        if init_spark_conf:
            self.all_conf["spark"] = init_spark_conf
        if init_logger_conf:
            self.all_conf["logger"] = init_logger_conf

        self.conf = copy.deepcopy(self.all_conf["app"])
        self.spark = self._prepare_spark(spark)
        self.logger = self._prepare_logger()
        self.dbutils = self.get_dbutils()
        self._log_conf()

    @staticmethod
    def _create_spark_conf(spark_conf: Dict) -> SparkConf:
        """Function to create SparkConf.

        Take all of spark conf in conf and Create SparkConf.
        WIP (may change in the future).

        Args:
            spark_conf (Dict): Spark conf.

        Returns:
            SparkConf: SparkConf created using configuration in self.conf of spark_conf section

        """
        sp_config_list: List[Any] = list(spark_conf.items())
        sp_config = SparkConf().setAll(sp_config_list)
        return sp_config

    def _prepare_spark(self, spark: SparkSession) -> SparkSession:
        """Function to get SparkSession.

        If the input spark is None, Create a SparkSession else Use the input spark.
        Use data_processor_name and main_transformation_name in conf to determine an appName.
        Apply spark conf if there is any in conf. WIP (may change in the future).

        Args:
            spark (SparkSession): An input SparkSession.

        Returns:
            SparkSession: Use 'spark' from Args if provided, create a SparkSession otherwise.

        """
        if not spark:
            spark_conf = self.all_conf.get("spark")
            spark_builder = SparkSession.builder

            # Get appName from the configuration file.
            spark_builder.appName(
                f"{self.conf[self.module_name]['data_processor_name']}.{self.conf[self.module_name]['main_transformation_name']}"
            )

            if spark_conf is not None:
                sp_config = self._create_spark_conf(spark_conf)
                spark_builder.config(conf=sp_config)

            spark_ss = spark_builder.getOrCreate()
            return spark_ss
        else:
            return spark

    def get_dbutils(self) -> Optional[Any]:
        """Function to get DBUtils.

        Get DBUtils and log the status.

        Returns:
            DBUtils if success, None otherwise.

        """
        utils = get_dbutils(self.spark)

        if not utils:
            self.logger.warn("No DBUtils defined in the runtime")
        else:
            self.logger.info("DBUtils class initialized")

        return utils

    def _provide_all_config(self, conf_path: str) -> Dict:
        """Function to get conf.

        Get a conf file from pipeline dir, read a conf file ,and replacing "conf:" references.

        Args:
            conf_path (str): A conf folder path.

        Returns:
            Dict: Conf from safe_load yaml.

        """

        conf_files = get_pipeline_conf_files(conf_path, self.module_name)
        conf_dict = self._read_all_config(conf_files)

        replaced_dict = replace_conf_reference(conf_dict, conf_path)
        return replaced_dict

    @staticmethod
    def _read_all_config(conf_files: Union[str, List]) -> Dict[str, Any]:
        """Function to read a conf file.

        Read files using __subclasses__ of ConfFileReader.

        Args:
            conf_file (str): A conf path.

        Returns:
            Dict: Conf loaded from conf_file with keys as file names and values as config values.

        """

        config = {}

        for each_reader in ConfFileReader.__subclasses__():
            each_conf = each_reader(conf_file_paths=conf_files).read_file()
            config.update(each_conf)

        return config

    def _prepare_logger(self) -> Any:
        """Function to get a logger.

        Return a log4j logger.

        Returns:
            Log4j logger from self.spark.

        """
        log4j_logger = self.spark._jvm.org.apache.log4j  # noqa
        return log4j_logger.LogManager.getLogger(self.__class__.__name__)

    def _log_conf(self) -> None:
        """Function to log the detail of conf.

        Log the detail of conf, for each key and item.

        """
        # log parameters
        self.logger.info("Launching job with configuration parameters:")
        for key, item in self.conf.items():
            self.logger.info("\t Parameter: %-30s with value => %-30s" % (key, item))

    @abstractmethod
    def launch(self) -> Any:
        """Main method of the job.

        Note:
            The main function of the class that uses this ABC should be named 'launch' as well.

        """
        pass
