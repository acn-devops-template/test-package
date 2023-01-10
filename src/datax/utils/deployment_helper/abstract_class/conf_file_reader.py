# import: standard
import json
import os
import pathlib
from abc import ABC
from abc import abstractmethod
from typing import Any
from typing import Dict
from typing import List
from typing import Union

# import: external
import yaml


class ConfFileReader(ABC):
    """Abstract base class for reading config files."""

    def __init__(
        self,
        conf_file_paths: Union[str, List],
    ) -> None:
        """__init__ function of FileReader.

        Args:
            file_paths (Union[str, List]):  Input file paths.

        """
        self.postfix_file_dict = self._get_postfix_file_dict(conf_file_paths)

    @staticmethod
    def _get_postfix_file_dict(file_paths: Union[str, List]) -> Dict:
        """Get the postfix file dictionary.

        Args:
            file_paths (Union[str, List]):  Input file paths.

        Returns:
            Dict: Postfix file dictionary.

        """

        if isinstance(file_paths, str):
            file_paths = [file_paths]
        postfix_file_dict: Dict[str, Any] = {}
        for file_path in file_paths:
            postfix = (os.path.basename(file_path).split(".")[-1]).lower()

            value = postfix_file_dict.get(postfix)

            if not value:
                postfix_file_dict[postfix] = [file_path]
            else:
                value.append(file_path)

        return postfix_file_dict

    @abstractmethod
    def read_file(self) -> Dict:
        """Base method for reading config files."""
        pass


class YAMLReader(ConfFileReader):
    """Subclass that inherits from ConfFileReader class."""

    def read_file(self) -> Dict:
        """Read a YAML config file and return a dictionary of configuration parameters.

        Returns:
            Dict: A dictionary of configuration parameters.

        """

        yaml_list = self.postfix_file_dict.get("yaml")
        yml_list = self.postfix_file_dict.get("yml")

        if yaml_list is not None and yml_list is not None:
            conf_files = yaml_list + yml_list
        elif yaml_list is None and yml_list is None:
            return {}
        else:
            conf_files = yaml_list or yml_list

        yaml_dict = {}
        for each_file in conf_files:
            file_name = os.path.basename(each_file).split(".")[0]

            conf_txt = pathlib.Path(each_file).read_text()
            config = yaml.safe_load(conf_txt)
            yaml_dict[file_name] = config

        return yaml_dict


class JSONReader(ConfFileReader):
    """Subclass that inherits from ConfFileReader class."""

    def read_file(self) -> Dict:
        """Read a JSON config file and return a dictionary of configuration parameters.

        Returns:
            Dict: A dictionary of configuration parameters.

        """

        json_list = self.postfix_file_dict.get("json")

        if json_list is None:
            return {}
        else:
            conf_files = json_list

        json_dict = {}
        for each_file in conf_files:
            file_name = os.path.basename(each_file).split(".")[0]

            conf_txt = pathlib.Path(each_file).read_text()
            config = json.loads(conf_txt)
            json_dict[file_name] = config

        return json_dict
