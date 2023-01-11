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

        Recieve a list containing config file paths and turn in to a dictionary
        with keys as file extension names and values as lists containing file paths.

        Args:
            file_paths (Union[str, List]):  Input file paths.

        """
        self.postfix_file_dict = self._get_postfix_file_dict(conf_file_paths)

    @staticmethod
    def _get_postfix_file_dict(file_paths: Union[str, List]) -> Dict:
        """Get the postfix file dictionary with keys as file extension names
        and values as lists containing file paths.

        Args:
            file_paths (Union[str, List]):  Input file paths.

        Returns:
            Dict: Postfix file dictionary.

        """

        file_paths = [file_paths] if isinstance(file_paths, str) else file_paths

        # uses the os.path.splitext() to get the file extension
        # and dict.setdefault() method to append the file path to the dictionary.
        postfix_file_dict: Dict[str, Any] = {}
        for file_path in file_paths:
            postfix = os.path.splitext(file_path)[1][1:].lower()
            postfix_file_dict.setdefault(postfix, []).append(file_path)

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

        conf_files = self.postfix_file_dict.get("yaml", []) + self.postfix_file_dict.get(
            "yml", []
        )
        if not conf_files:
            return {}

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

        # return an empty dictionary if json_list is None
        if json_list is None:
            return {}

        json_dict = {}
        for each_file in json_list:
            # extract file name without extension
            file_name = os.path.basename(each_file).split(".")[0]
            conf_txt = pathlib.Path(each_file).read_text()
            config = json.loads(conf_txt)
            json_dict[file_name] = config

        return json_dict
