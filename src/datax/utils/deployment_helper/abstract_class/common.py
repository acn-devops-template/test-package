"""abstract_class common module"""

# import: standard
import logging
import logging.config
import sys
from abc import ABC
from abc import abstractmethod
from typing import Any
from typing import Dict
from typing import List
from typing import Optional

# import: pyspark
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession

# import: datax in-house
from datax.utils.deployment_helper.converter.log4j_handler import Log4JProxyHandler
from datax.utils.deployment_helper.converter.path_adjuster import (
    recursive_read_pipeline_conf,
)
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
        * self.conf_audit is a audit configuration if provided.
        * self.conf_sensor is a sensor configuration if provided.

    """

    def __init__(
        self,
        module_name: str,
        conf_path: Optional[str] = None,
        spark: Optional[SparkSession] = None,
        init_conf_app: Optional[Dict] = None,
        init_conf_spark: Optional[Dict] = None,
        init_conf_logger: Optional[Dict] = None,
        init_conf_audit: Optional[Dict] = None,
        init_conf_sensor: Optional[Dict] = None,
        activate_audit: Optional[bool] = False,
        activate_sensor: Optional[bool] = False,
    ) -> None:
        """__init__ function of Task class.

        Set following useful objects: self.spark, self.logger, self.dbutils, self.conf_app, self.conf_spark, self.conf_logger, self.conf_audit, self.conf_sensor.

        Args:
            module_name (str): Use this input as self.module_name.
            conf_path (Optional[str]):  Use this path to find conf files.
            spark (Optional[SparkSession]): Use this input as self.spark if provided, create a SparkSession otherwise.
            init_conf_app (Optional[Dict]): If provided, use this input as self.conf_all["app"].
            init_conf_spark (Optional[Dict]): If provided, use this input as self.conf_all["spark"], otherwise None.
            init_conf_logger (Optional[Dict]): If provided, use this input as self.conf_all["logger"], otherwise None.
            init_conf_audit (Optional[Dict]): If provided, use this input as self.conf_all["audit"], otherwise None.
            init_conf_sensor (Optional[Dict]): If provided, use this input as self.conf_all["sensor"], otherwise None.
            activate_audit(Optional[bool]): If provided, use this boolean input to activate audit, otherwise False.
            activate_sensor (Optional[bool]): If provided, use this boolean input to activate sensor, otherwise False.

        """
        self.module_name = module_name
        conf_all = self._provide_conf_all(conf_path) if conf_path else {}
        if init_conf_app:
            conf_all["app"] = init_conf_app

        conf_all["spark"] = init_conf_spark or conf_all.get("spark", {})
        conf_all["logger"] = init_conf_logger or conf_all.get("logger", {})
        conf_all["audit"] = init_conf_audit or conf_all.get("audit", {})
        conf_all["audit"]["activate"] = activate_audit
        conf_all["sensor"] = init_conf_sensor or conf_all.get("sensor", {})
        conf_all["sensor"]["activate"] = activate_sensor

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

            # Get appName from the configuration file with or without module name section.
            module_conf = app_conf.get(self.module_name)
            data_processor_name = app_conf.get("data_processor_name") or module_conf.get(
                "data_processor_name"
            )
            main_transformation_name = app_conf.get(
                "main_transformation_name"
            ) or module_conf.get("main_transformation_name")

            # Get appName from the configuration file.
            spark_builder.appName(f"{data_processor_name}.{main_transformation_name}")

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

        conf_dict = recursive_read_pipeline_conf(conf_path, self.module_name)
        replaced_dict = replace_conf_reference(conf_dict, conf_path)
        return replaced_dict

    def _prepare_logger(self) -> logging.Logger:
        """The method for getting a logger instance.

        The log4j handler is added to root of the logging.
        This handler serves as a transmitter that helps forward Python logging messages to the log4j.
        As a result, logging messages are always displayed on both the Standard console and the Log4j console.

        Moreover, this method allows users to pass the logger configuration file `conf_logger`.
        If logger config file `conf_logger` is provided, the logging configuration will be based on it using `dictConfig`.
        Otherwise, a default logging configuration will be used with an `INFO` log level, a specific format, and a `StreamHandler` for output.

        Returns:
            logging.Logger: A logger instance for logging messages.
        """
        logger = logging.getLogger(
            f"{self.__class__.__module__}.{self.__class__.__qualname__}"
        )

        # Set logging configuration.
        if self.conf_logger:  # type: ignore
            logging.config.dictConfig(self.conf_logger)  # type: ignore
        else:

            stdout_handler = logging.StreamHandler(stream=sys.stdout)
            stdout_handler.setLevel(logging.INFO)

            stderr_handler = logging.StreamHandler(stream=sys.stderr)
            stderr_handler.setLevel(logging.ERROR)

            log4j_handler = Log4JProxyHandler(self.spark)

            logging.basicConfig(
                level=logging.INFO,
                format="%(asctime)s ----- %(levelname)s ----- %(name)s ----- %(filename)s -- %(message)s",
                datefmt="%Y-%m-%dT%H:%M:%S%z",
                handlers=[stderr_handler, stdout_handler, log4j_handler],
            )

        return logger

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
