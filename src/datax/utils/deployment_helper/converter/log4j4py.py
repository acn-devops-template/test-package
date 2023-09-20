# import: standard
import logging
from logging import Handler
from logging import LogRecord

# import: pyspark
from pyspark.sql import SparkSession


class Log4JProxyHandler(Handler):
    """Handler to forward messages to log4j."""

    def __init__(self, spark_session: SparkSession) -> None:
        """Initialize handler with a log4j logger."""
        Handler.__init__(self)
        self.Logger = spark_session._jvm.org.apache.log4j.Logger

    def emit(self, record: LogRecord) -> None:
        """Emit a log message."""
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
        """Close the logger."""
