# import: standard
import logging
from logging import Handler
from logging import LogRecord

# import: pyspark
from pyspark.sql import SparkSession


class Log4JProxyHandler(Handler):
    """Handler Class to forward messages from Python logging to log4j."""

    def __init__(self, spark: SparkSession) -> None:
        """The initialize method of the handler is initialized with a log4j logger.
        Args:
            spark (SparkSession): An input SparkSession.
        """
        Handler.__init__(self)
        self.Logger = spark._jvm.org.apache.log4j.Logger

    def emit(self, record: LogRecord) -> None:
        """The method for forwarding a log message in log4j.
        Args:
            record (LogRecord): A log record from Python logging.
        """
        logger = self.Logger.getLogger(record.name)
        if record.levelno >= logging.CRITICAL:
            logger.fatal(record.getMessage())
        elif record.levelno >= logging.ERROR:
            logger.error(record.getMessage())
        elif record.levelno >= logging.WARNING:
            logger.warn(record.getMessage())
        elif record.levelno >= logging.INFO:
            logger.info(record.getMessage())
        elif record.levelno >= logging.DEBUG:
            logger.debug(record.getMessage())

    def close(self) -> None:
        """The method for closing the log4j handler."""
