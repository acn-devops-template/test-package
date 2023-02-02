""" path adjuster modules """
# import: standard
import os
import pathlib
from typing import Any
from typing import Dict
from typing import List
from typing import Union

# import: datax in-house
from datax.utils.deployment_helper.abstract_class.conf_file_reader import ConfFileReader


def find_conf_path(
    file: str,
    conf_dir_name: str = "conf",
    src_dir_name: str = "src",
) -> str:
    """Function to get a conf path.

    Get a conf path from __file__ and apply pathlib as_posix to the path.
    Use split to get the base path and concat with "/conf"

    Args:
        file (str): __file__
        conf_dir_name (str): conf directory name (e.g. sub_dir/conf).
        src_dir_name (str): src directory name (e.g. sub_dir/src).

    Returns:
        str: A conf path.

    Raises:
        FileNotFoundError: If the conf path is not found.

    """

    if conf_dir_name.startswith("/"):
        conf_dir_name = conf_dir_name[1:]

    file_abs_path = pathlib.Path(os.path.abspath(file)).as_posix()

    if "/site-packages/" in file_abs_path:
        base_path = find_parent_path_of_dir_bottom_up(file_abs_path, "site-packages")
        base_path = find_folder_from_path_top_down(base_path, "site-packages")
    else:
        base_path = find_parent_path_of_dir_bottom_up(file_abs_path, src_dir_name)

    conf_path = find_folder_from_path_top_down(base_path, conf_dir_name)

    if os.path.isdir(conf_path):
        return conf_path
    else:
        raise FileNotFoundError(
            f"{conf_path} is not a directory. Please check the conf_dir_name: {conf_dir_name}"
        )


def find_parent_path_of_dir_bottom_up(
    posix_path: str,
    dir_name: str,
) -> str:
    """Function to get a dir path.

    Get a dir path from posix_path and use a loop to find a parent directory until the base directory is found.

    Args:
        posix_path (str): Absolute posix path.
        dir_name (str): A base directory name.

    Returns:
        str: A directory path based on dir_name.

    Raises:
        FileNotFoundError: If the dir_name is not found.

    """

    path_list = posix_path.split("/")
    posix_path_copy = posix_path
    for _ in range(len(path_list)):
        path, folder = os.path.split(posix_path_copy)
        if folder == dir_name:
            return path
        posix_path_copy = path
    raise FileNotFoundError(f"{dir_name} folder not found from {posix_path}")


def find_folder_from_path_top_down(
    path: str,
    folder_name: str,
) -> str:
    """
    Finds the path to a given folder by name in a directory tree, starting from the provided path.

    Args:
        path (str): The root path to start searching from.
        folder_name (str): The name of the folder to search for.

    Returns:
        str: The path to the folder, or None if the folder is not found.

    Raises:
        FileNotFoundError: If the folder is not found.

    """

    for root, dirs, _ in os.walk(path):
        if folder_name in dirs:
            return pathlib.Path(os.path.join(root, folder_name)).as_posix()
    raise FileNotFoundError(f"{folder_name} not found in {path}")


def replace_conf_reference(
    conf_dict: Dict,
    conf_path: str,
) -> Dict:
    """Function to replace "conf:" references.

    Read every value in conf_dict and if any starts with "conf:", that "conf:" will be replaced with conf_path.

    Args:
        conf_dict (Dict): Input conf dict
        conf_path (str): A configuration path.

    Returns:
        Dict: Replaced conf Dict.

    """
    prefix_name = "conf:"

    # replace any value in dict that starts with "conf:"
    for each_key, each_val in conf_dict.items():
        if isinstance(each_val, str) and each_val.startswith(prefix_name):
            conf_dict[each_key] = each_val.replace(prefix_name, conf_path)
        elif isinstance(each_val, Dict):
            replace_conf_reference(each_val, conf_path)

    return conf_dict


def get_pipeline_conf_files(
    conf_path: str,
    module_name: str,
) -> List[str]:
    """Function to get all conf file paths.

    Find conf files based on Glob pattern '**/*pipeline*/**/{module_name}/*'.
    The root path is based on conf_path.

    Args:
        conf_path (str): A config folder path.

    Returns:
        List[str]: A list of conf paths from the pipeline dir.

    """
    # pipeline conf file glob pattern
    pl_glob = f"**/*pipeline*/**/{module_name}/*"

    # for testing via Databricks and use DBFS path
    conf_dir = conf_path.replace("dbfs:", "/dbfs") if "dbfs:" in conf_path else conf_path

    conf_list = [
        str(path.as_posix())
        for path in pathlib.Path(conf_dir).glob(pl_glob)
        if path.is_file()
    ]

    if not conf_list:
        raise FileNotFoundError(
            f"Cannot find the pipeline conf in {conf_dir} with {pl_glob}"
        )
    else:
        file_names = [os.path.basename(path).split(".", 1)[0] for path in conf_list]
        if len(set(file_names)) != len(file_names):
            raise ValueError(f"Found more than one conf file per type, {conf_list}")

    return conf_list


def read_conf_all(conf_files: Union[str, List]) -> Dict[str, Any]:
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
