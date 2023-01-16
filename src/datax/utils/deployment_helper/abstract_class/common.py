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
        * self.spark is a SparkSession.
        * self.dbutils provides access to the DBUtils.
        * self.logger provides access to the Spark-compatible logger.
        * self.conf_app provides application configuration of the job.
        * self.conf_spark is a spark configuration if provided.
        * self.conf_logger is a logger configuration if provided.
        * self.conf_deequ is a deequ configuration if provided.

    """

    def __init__(
        self,
        module_name: str,
        conf_path: Optional[str] = None,
        spark: Optional[SparkSession] = None,
        init_conf_app: Optional[Dict] = None,
        init_conf_spark: Optional[Dict] = None,
        init_conf_logger: Optional[Dict] = None,
        init_conf_deequ: Optional[Dict] = None,
    ) -> None:
        """__init__ function of Task class.

        Set following useful objects: self.spark, self.logger, self.dbutils, self.conf_app, self.conf_spark, self.conf_logger, self.conf_deequ.

        Args:
            module_name (str): Use this input as self.module_name.
            conf_path (Optional[str]):  Use this path to find conf files.
            spark (Optional[SparkSession]): Use this input as self.spark if provided, create a SparkSession otherwise.
            init_conf_app (Optional[Dict]): If provided, use this input as self.conf_all["app"].
            init_conf_spark (Optional[Dict]): If provided, use this input as self.conf_all["spark"], otherwise None.
            init_conf_logger (Optional[Dict]): If provided, use this input as self.conf_all["logger"], otherwise None.
            init_conf_deequ (Optional[Dict]): If provided, use this input as self.conf_all["deequ"], otherwise None.

        """
        self.module_name = module_name
        conf_all = self._provide_conf_all(conf_path) if conf_path else {}

        if init_conf_app:
            conf_all["app"] = init_conf_app

        conf_all["spark"] = init_conf_spark or conf_all.get("spark", {})
        conf_all["logger"] = init_conf_logger or conf_all.get("logger", {})
        conf_all["deequ"] = init_conf_deequ or conf_all.get("deequ", {})

        # Set conf to attributes
        self.conf_all = conf_all
        for each_key in conf_all.keys():
            setattr(self, f"conf_{each_key}", conf_all[each_key])

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
            SparkConf: SparkConf created using configuration in spark conf file or init_conf_spark variable.

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
            app_conf = self.conf_all.get("app")  # required
            spark_conf = self.conf_all.get("spark")  # optional
            spark_builder = SparkSession.builder

            # Get appName from the configuration file.
            spark_builder.appName(
                f"{app_conf[self.module_name]['data_processor_name']}.{app_conf[self.module_name]['main_transformation_name']}"
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

    def _provide_conf_all(self, conf_path: str) -> Dict:
        """Function to get conf.

        Get a conf file from pipeline dir, read a conf file ,and replacing "conf:" references.

        Args:
            conf_path (str): A conf folder path.

        Returns:
            Dict: Conf from safe_load yaml.

        """

        conf_files = get_pipeline_conf_files(conf_path, self.module_name)
        conf_dict = self._read_conf_all(conf_files)

        replaced_dict = replace_conf_reference(conf_dict, conf_path)
        return replaced_dict

    @staticmethod
    def _read_conf_all(conf_files: Union[str, List]) -> Dict[str, Any]:
        """Function to read a conf file.

        Read all config files using __subclasses__ of ConfFileReader.

        Args:
            conf_files (Union[str, List]): A conf path or a list of paths.

        Returns:
            Dict[str, Any]: Conf loaded from conf_file with keys as file names and values as config values.

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
        for key, item in self.conf_all.items():
            self.logger.info(
                "\t configuration type: %-30s with value => %-30s" % (key, item)
            )

    @abstractmethod
    def launch(self) -> Any:
        """Main method of the job.

        Note:
            The main function of the class that uses this ABC should be named 'launch' as well.

        """
        pass
