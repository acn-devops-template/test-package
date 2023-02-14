""" date adjuster modules """

# import: standard
import fnmatch
from datetime import datetime
from typing import Union

# import: datax in-house
from datax.utils.deployment_helper.converter.path_adjuster import find_conf_path
from datax.utils.deployment_helper.converter.path_adjuster import get_pipeline_conf_files
from datax.utils.deployment_helper.converter.path_adjuster import read_conf_all

# import: external
from dateutil.relativedelta import relativedelta


def check_input_date_format(
    input_date: str, to_datetime: bool = False
) -> Union[str, datetime]:
    """Function to check input_date format.

    Check if the input string is in a correct datetime format `YYYY-MM-DD`.
    Return datetime object if to_datetime set to True.

    Args:
        input_date (str): The input date as string.
        to_datetime (bool, optional): Flag to cast validated input date into datetime.

    Returns:
        Union[str, datetime]: Input date in string format `YYYY-MM-DD` or datetime object.

    Raises:
        ValueError: If the input_date is not in the correct format `YYYY-MM-DD`.

    """

    try:
        parsed_date = datetime.strptime(input_date, "%Y-%m-%d")
        return parsed_date if to_datetime else parsed_date.strftime("%Y-%m-%d")
    except ValueError:
        raise ValueError(
            f"Incorrect date format for `{input_date}`, the date should have format of YYYY-MM-DD"
        )


def get_module_conf(conf_path: str, pipeline: str) -> dict:
    """Function to get pipeline app config.

    Use get_pipline_conf_files to get pipeline conf files, read and return only app conf.

    Args:
        conf_path (str): A conf folder path.
        pipeline (str): Pipeline name.

    Returns:
        dict: Pipeline App conf.

    """

    conf_file_name = "app"
    conf_files = get_pipeline_conf_files(conf_path, pipeline)

    # Select the files that match the specified conf_file_name
    selected_conf_files = [
        file for file in conf_files if fnmatch.fnmatch(file, f"*/{conf_file_name}.*")
    ]
    app_conf = read_conf_all(selected_conf_files)

    return app_conf.get(conf_file_name)


def _convert_offset_into_dict(offset_string: str) -> dict:
    """Function to convert a string representation of date offset into a dictionary to be passed as kwargs.

    Args:
        offset_string (str): String date offset.

    Returns:
        dict: Date offset dict.

    Raises:
        ValueError: If the string is in an incorrect format.

    Example:
        >>> _convert_offset_into_dict("key1=value1, key2=value2")
        {'key1': 'value1', 'key2': 'value2'}

    """

    offsets = offset_string.replace(" ", "").split(",")
    offsets_pairs = (offset.split("=") for offset in offsets)
    offset_dict = dict((key, int(value)) for key, value in offsets_pairs)

    return offset_dict


def compute_run_date(run_date: str, module: str) -> str:
    """Function to compute run date by adjusting with the offset defined in the module configuration.

    Args:
        run_date (str): Input run date.
        module (str): Module name.

    Returns:
        str: The adjusted run date in the format of "YYYY-MM-DD".

    """

    conf_path = find_conf_path(__file__)
    module_conf = get_module_conf(conf_path, module)

    date_offset = module_conf.get("date_offset", None) or module_conf.get(module).get(
        "date_offset", None
    )
    # If date_offset exists, convert it into dict, otherwise default offset to {"days": -2}
    offset = (
        _convert_offset_into_dict(date_offset)
        if date_offset is not None
        else {"days": -2}
    )

    module_run_date = datetime.strptime(run_date, "%Y-%m-%d") + relativedelta(**offset)

    return module_run_date.strftime("%Y-%m-%d")
