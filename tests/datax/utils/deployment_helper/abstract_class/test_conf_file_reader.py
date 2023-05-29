""" conf_file_reader tests """

# import: standard
import json
import os
import pathlib
import unittest

# import: datax in-house
from datax.utils.deployment_helper.abstract_class.conf_file_reader import J2Reader
from datax.utils.deployment_helper.abstract_class.conf_file_reader import JSONReader
from datax.utils.deployment_helper.abstract_class.conf_file_reader import YAMLReader

# import: external
import yaml
from jinja2 import Template


class TestYAMLReader(unittest.TestCase):
    """Test Class for testing YAMLReader.

    Class for testing YAMLReader.

    Args:
        unittest.TestCase: An unittest TestCase.

    """

    def test_yaml_reader(self) -> None:
        """Test reading yaml files."""
        yaml_paths = [
            "tests/resources/conf_files/app.yml",
            "tests/resources/conf_files/spark.yaml",
        ]
        yaml_cls = YAMLReader(conf_file_paths=yaml_paths)
        yaml_conf = yaml_cls.read_file()

        app_conf = pathlib.Path(yaml_paths[0]).read_text()
        app_config = yaml.safe_load(app_conf)

        spark_conf = pathlib.Path(yaml_paths[1]).read_text()
        spark_config = yaml.safe_load(spark_conf)

        self.assertEqual(yaml_conf, {"app": app_config, "spark": spark_config})


class TestJ2Reader(unittest.TestCase):
    """Test Class for testing J2Reader.

    Class for testing J2Reader.

    Args:
        unittest.TestCase: An unittest TestCase.

    """

    def test_j2_reader(self) -> None:
        """Test reading Jinja2 files."""
        os.environ["ENVIRONMENT"] = "STG"
        mapping_dict = {"env": dict(os.environ)}
        j2_paths = [
            "tests/resources/conf_files/app.yml.j2",
            "tests/resources/conf_files/logger.json.j2",
        ]
        j2_cls = J2Reader(conf_file_paths=j2_paths)
        j2_conf = j2_cls.read_file()

        app_conf_content = pathlib.Path(j2_paths[0]).read_text()
        template = Template(app_conf_content)
        app_conf = template.render(mapping_dict)
        app_config = yaml.safe_load(app_conf)

        logger_conf_content = pathlib.Path(j2_paths[1]).read_text()
        template = Template(logger_conf_content)
        logger_conf = template.render(mapping_dict)
        logger_config = json.loads(logger_conf)

        self.assertEqual(j2_conf, {"app": app_config, "logger": logger_config})


class TestJSONReader(unittest.TestCase):
    """Test Class for testing JSONReader.

    Class for testing JSONReader.

    Args:
        unittest.TestCase: An unittest TestCase.

    """

    def test_json_reader(self) -> None:
        """Test reading json files."""
        json_path = "tests/resources/conf_files/logger.json"
        json_cls = JSONReader(conf_file_paths=json_path)
        json_conf = json_cls.read_file()

        logger_conf = pathlib.Path(json_path).read_text()
        logger_config = json.loads(logger_conf)

        self.assertEqual(json_conf, {"logger": logger_config})
