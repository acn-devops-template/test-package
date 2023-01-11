""" path adjuster modules """
# import: standard
import os
import pathlib
from typing import Any
from typing import Dict
from typing import List


def find_conf_path(file: str, to_conf: str = "conf") -> str:
    """Function to get a conf path.

    Get a conf path from __file__ and apply pathlib as_posix to the path.
    Use split to get the base path and concat with "/conf"

    Args:
        file (str): __file__
        to_conf (str): conf path to concat with a base path (e.g. sub_dir/conf).

    Returns:
        str: A conf path.

    """

    if to_conf.startswith("/"):
        to_conf = to_conf[1:]

    root_dir = pathlib.Path(os.path.abspath(file)).as_posix()

    if "/site-packages/" in root_dir:
        split_path = root_dir.rsplit("/", 4)
    else:
        split_path = root_dir.rsplit("/", 5)

    conf_path = f"{split_path[0]}/{to_conf}"

    if os.path.isdir(conf_path):
        return conf_path
    else:
        raise FileNotFoundError(f"Cannot find conf directory: {conf_path}")


def replace_conf_reference(conf_dict: Dict, conf_path: str) -> Dict:
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


def get_pipeline_conf_files(conf_path: str, module_name: str) -> List[str]:
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
