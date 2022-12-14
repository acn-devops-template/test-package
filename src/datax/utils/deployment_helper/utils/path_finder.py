""" path finder modules """
# import: standard
import os
import pathlib


def find_base_path(file: str) -> str:
    """Function to get a site-packages path.

    Get a base path from __file__ and apply pathlib as_posix to the path.

    Args:
        file (str): __file__

    Returns:
        str: A base path.

    """

    root_dir = pathlib.Path(os.path.abspath(file)).as_posix()

    if "/src/datax/" in root_dir:
        split_path = root_dir.split("/src/datax/", 1)[0]
    else:
        split_path = root_dir.split("/datax/", 1)[0]

    return split_path
