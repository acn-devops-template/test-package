"""Abstract base class for reading config files."""

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
from jinja2 import Template


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

    @abstractmethod
    def read_content(self, *args: Any, **kwargs: Dict[str, Any]) -> Union[str, Dict]:
        """Base method for reading config contents.

        Args:
            content (str): content of configuration
            mapping (Dict, optional): mapping for key values used only in J2Reader. Defaults to {}.

        Returns:
            Dict: output config
        """
        pass


class YAMLReader(ConfFileReader):
    """Subclass that inherits from ConfFileReader class."""

    def read_file(self) -> Dict:
        """Read a YAML config file and return a dictionary of configuration parameters
        with keys as file extension names and values as the config dict.

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
            config = self.read_content(content=conf_txt)
            yaml_dict[file_name] = config

        return yaml_dict

    def read_content(self, content: str) -> Union[str, Dict]:
        """Method for reading YAML configuration string.

        Args:
            content (str): content of configuration
            mapping (Dict, optional): mapping for key values used only in J2Reader. Defaults to {}.

        Returns:
            Dict: output config
        """
        return yaml.safe_load(content)


class JSONReader(ConfFileReader):
    """Subclass that inherits from ConfFileReader class."""

    def read_file(self) -> Dict:
        """Read a JSON config file and return a dictionary of configuration parameters
        with keys as file extension names and values as the config dict.

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
            config = self.read_content(content=conf_txt)
            json_dict[file_name] = config

        return json_dict

    def read_content(self, content: str) -> Union[str, Dict]:
        """Method for reading JSON configuration string.

        Args:
            content (str): content of configuration
            mapping (Dict, optional): mapping for key values used only in J2Reader. Defaults to {}.

        Returns:
            Dict: output config
        """
        return json.loads(content)


class J2Reader(ConfFileReader):
    """Subclass that inherits from ConfFileReader class."""

    def read_file(self) -> Dict:
        """Read a J2 template file and return a dictionary of configuration parameters
        with keys as file extension names and values as the config dict.

        Returns:
            Dict: A dictionary of configuration parameters.

        """

        conf_files = self.postfix_file_dict.get("j2", [])
        if not conf_files:
            return {}

        j2_dict = {}
        for each_file in conf_files:
            file_name_components = os.path.basename(each_file).split(".")
            file_name = file_name_components[0]
            original_file_format = file_name_components[1]
            
            content = pathlib.Path(each_file).read_text()
            conf_txt = self.read_content(content=content, mapping=dict(os.environ))
            conf_txt = str(conf_txt)

            if original_file_format in ["yml", "yaml"]:
                config = YAMLReader([]).read_content(content=conf_txt)
                j2_dict[file_name] = config
            elif original_file_format in ["json"]:
                config = JSONReader([]).read_content(content=conf_txt)
                j2_dict[file_name] = config

        return j2_dict

    def read_content(self, content: str, mapping: Dict) -> Union[str, Dict]:
        """Method for reading J2 configuration template and rendering the template.

        Args:
            content (str): content of configuration
            mapping (Dict, optional): mapping for key values used only in J2Reader. Defaults to {}.

        Returns:
            Dict: output config
        """
        template = Template(content)
        return template.render(mapping)
