"""abstract_class common module"""

# import: standard
import pathlib
import sys
from abc import ABC
from abc import abstractmethod
from argparse import ArgumentParser
from typing import Any
from typing import Dict

# import: pyspark
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession

# import: external
import yaml


def get_dbutils(spark: SparkSession):
    """
    Get DBUtils from pyspark.dbutils if error return None
    please note that this function is used in mocking by its name
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
    """
    This is an abstract class that provides handy interfaces to implement workloads (e.g. jobs or job tasks).
    Create a child from this class and implement the abstract launch method.
    Class provides access to the following useful objects:
    * self.spark is a SparkSession
    * self.dbutils provides access to the DBUtils
    * self.logger provides access to the Spark-compatible logger
    * self.conf provides access to the parsed configuration of the job
    """

    def __init__(self, spark=None, init_conf=None, module_name=None, conf_dir="./conf"):
        """
        Set following useful objects: self.conf, self.spark, self.logger, self.dbutils
        Input parameters:
        * spark is a SparkSession; if this input is not None, use the input spark as self.spark
        * init_conf is a conf dict; if this input is not None, use init_conf as self.conf
        * module_name is a conf dict; if this input is not None, use module_name as self.module_name (mainly for testing)
        * conf_dir is a path to conf dir; default vault is './conf' (mainly for testing)
        """
        self._set_config = False
        self.conf_dir = conf_dir

        if module_name:
            self.module_name = module_name
        else:
            self.module_name = self._get_module_name()

        if init_conf:
            self.conf = init_conf
        else:
            self.conf = self._provide_config()

        self.spark = self._prepare_spark(spark)
        self.logger = self._prepare_logger()
        self.dbutils = self.get_dbutils()
        self._log_conf()

    def _get_module_name(self):
        """
        Return module name from 'module' argument in ArgumentParser
        """
        ps = ArgumentParser()
        ps.add_argument("--module", required=False, type=str, help="module name")
        module_nsp = ps.parse_known_args(sys.argv[1:])[0]

        if module_nsp.module is None:
            raise ValueError(" module argument is not found ")
        return module_nsp.module

    def _create_spark_conf(self):
        """
        WIP (may change in the future)
        Take all of spark conf in conf and Create SparkConf
        """
        sp_config_list = list(self.conf[self.module_name]["spark_config"].items())
        sp_config = SparkConf().setAll(sp_config_list)
        return sp_config

    def _check_for_spark_conf(self):
        """
        WIP (may change in the future)
        If there is a spark conf in conf, set self._set_config to True
        """
        if "spark_config" in self.conf[self.module_name].keys():
            if len(self.conf[self.module_name]["spark_config"]) > 0:
                self._set_config = True

    def _prepare_spark(self, spark) -> SparkSession:
        """
        WIP (may change in the future)
        If the input spark is None, Create a SparkSession else Use the input spark.
        Use data_processor_name and main_transformation_name in conf to determine an appName.
        Apply spark conf if there is any in conf.
        """
        if not spark:
            # TODO: Get appName from the configuration file.
            dp_name = self.conf[self.module_name]["data_processor_name"]
            mt_name = self.conf[self.module_name]["main_transformation_name"]
            self._check_for_spark_conf()

            if self._set_config:
                sp_config = self._create_spark_conf()
                spark_ss = (
                    SparkSession.builder.appName(f"{dp_name}.{mt_name}")
                    .config(conf=sp_config)
                    .getOrCreate()
                )
            else:
                spark_ss = SparkSession.builder.appName(
                    f"{dp_name}.{mt_name}"
                ).getOrCreate()
            return spark_ss
        else:
            return spark

    def get_dbutils(self):
        """
        Get DBUtils and log the status
        """
        utils = get_dbutils(self.spark)

        if not utils:
            self.logger.warn("No DBUtils defined in the runtime")
        else:
            self.logger.info("DBUtils class initialized")

        return utils

    def _provide_config(self):
        """
        Get a conf file from pipeline dir and Read a conf file
        """
        conf_file = self._get_conf_file()
        return self._read_config(conf_file)

    def _get_conf_file(self):
        """
        Find a conf file based on Glob pattern '**/*pipeline*/**/{module_name}.yml' (yml or yaml)
        The root path is based on self.conf_dir
        """
        # the tuple of file types
        types = (
            f"**/*pipeline*/**/{self.module_name}.yml",
            f"**/*pipeline*/**/{self.module_name}.yaml",
        )

        files_grabbed = []
        for each in types:
            files_grabbed.extend(pathlib.Path(self.conf_dir).glob(each))

        conf_list = [x for x in files_grabbed if x.is_file()]

        if len(conf_list) == 0:
            raise FileNotFoundError(
                f"""Cannot file the pipeline conf via this glob pattern: '{self.conf_dir}/**/*pipeline*/**/{self.module_name}.yml', please make sure the module name is correct and the configuration file exists"""
            )
        elif len(conf_list) > 1:
            raise ValueError(f" Found more than one conf, {conf_list} ")

        return conf_list[0]

    @staticmethod
    def _read_config(conf_file) -> Dict[str, Any]:
        """
        Return a conf using yaml safe_load
        """
        config = yaml.safe_load(pathlib.Path(conf_file).read_text())
        return config

    def _prepare_logger(self):
        """
        Return a log4j logger
        """
        log4j_logger = self.spark._jvm.org.apache.log4j  # noqa
        return log4j_logger.LogManager.getLogger(self.__class__.__name__)

    def _log_conf(self):
        """
        Log the detail of conf
        """
        # log parameters
        self.logger.info("Launching job with configuration parameters:")
        for key, item in self.conf.items():
            self.logger.info("\t Parameter: %-30s with value => %-30s" % (key, item))

    @abstractmethod
    def launch(self):
        """
        Main method of the job.
        :return:
        """
        pass
