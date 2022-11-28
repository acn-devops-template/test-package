"""decorator set_default_obj, set_pipeline_obj, set_tfm_obj modules"""

# import: standard
import functools
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import Optional
from typing import Sequence
from typing import Tuple
from typing import TypeVar
from typing import cast

# import: pyspark
from pyspark.sql import SparkSession

# import: datax in-house
from datax.utils.deployment_helper.validation.common import (
    PipelineConfigArgumentValidators,
)
from datax.utils.deployment_helper.validation.common import (
    TransformationConfigArgumentValidator,
)

# func type annotation
F = TypeVar("F", bound=Callable[..., Any])

# dict to store obj created from __main__
_default_obj: Dict = {"from_handler": False, "from_pipeline": False}
# dict to store obj created from a pipeline
_pipeline_obj: Dict = {}


def pop_var_dict(co_var: List, _var_dict: Dict, args: Tuple, kwargs: Dict) -> None:
    """Function to pop keys of _var_dict, dict containing vars

    If there are hard-coded vars which are called from a pipeline class or handler,
    those hard-coded vars will take priority over the same vars from conf.
    If there are vars that the Tfm/Pipeline class does not define in the function,
    those vars will be taken out from kwargs that will be provided.

    Args:
        co_var (List) : List of vars that function uses
        _var_dict (Dict): Input variable dict
        args (Tuple): Input arguments.
        kwargs (Dict): Input keyword arguments.

    """
    # variables in tfm class overwrite config variables
    if len(kwargs) > 0:
        for kw_key in kwargs.keys():
            if kw_key in _var_dict.keys():
                _var_dict.pop(kw_key, None)
    if len(args) > 0:
        args_name = co_var[0 : len(args)]
        for ar_key in args_name:
            if ar_key in _var_dict.keys():
                _var_dict.pop(ar_key, None)

    # pop if tfm class doesn't define these vars, e.g. 'spark','logger','dbutils'.
    for each_var in list(_var_dict.keys()):
        if each_var not in co_var:
            _var_dict.pop(each_var, None)


def set_pipeline_obj(function: F) -> F:
    """Decorator function for setting pipeline objects.

    Take conf, logger, spark from _default_obj (created from __main__)
    and pass these variables to function as keyword arguments
    but if not being run from pipeline, this decorator does nothing.

    Args:
        function (Callable): A __init__ function of a pipeline class.

    Returns:
        Callable: 'wrap_set_pipeline_obj' function.

    """

    def _create_pipeline_var_dict(args: Tuple, kwargs: Dict) -> Dict:
        """Function for creating a pipeline variable dict.

        Create a dict that contains key:value of all variables
        that were passed into __init__ by users

        Args:
            args (Tuple): Input arguments.
            kwargs (Dict): Input keyword arguments.

        Returns:
            Dict: A dict containing key:vaule of pipeline variables that users provide.

        """
        _pipeline_var_dict = {}
        _pipeline_var_list = [i for i in function.__code__.co_varnames if i != "self"]
        if len(args) > 0:
            for each_ar in range(0, len(args)):
                _pipeline_var_dict[_pipeline_var_list[each_ar]] = args[each_ar]
        if len(kwargs) > 0:
            for key_kw, value_kw in kwargs.items():
                _pipeline_var_dict[key_kw] = value_kw
        return _pipeline_var_dict

    def _set_default_vars_to_pipeline_obj(
        _pipeline_var_dict: Dict, var_list: List
    ) -> None:
        """Function for setting '_pipeline_obj' dict.

        Assign vars in ['conf', 'logger', 'dbutils', 'spark'] to '_pipeline_obj' dict.

        Args:
            _pipeline_var_dict (Dict): A dict containing key:vaule from '_create_pipeline_var_dict'.
            var_list (List): List of keys to select, e.g. ["conf", "logger", "dbutils", "spark"]

        """
        _pl_obj_dict = {}
        for each_obj in var_list:
            if each_obj in _pipeline_var_dict.keys():
                _pl_obj_dict[each_obj] = _pipeline_var_dict[each_obj]

        _pl_spark_ss = _pl_obj_dict.pop("spark", None)
        _pipeline_obj["default"] = _pl_obj_dict
        _pipeline_obj["spark"] = _pl_spark_ss

    def _get_default_spark_conf(
        spark: SparkSession, conf: Dict
    ) -> Sequence[Tuple[Any, Any]]:
        """Function for changing spark conf on the fly.

        Reserved for changing spark conf on the fly (WIP).

        Args:
            spark (SparkSession): Input SparkSession.
            conf (Dict): Input pipeline conf.

        Returns:
            Sequence[Tuple]: A List of tuples of spark conf.

        """
        spark_conf_list = []
        default_conf_list = []

        def _collect_all_spark_conf(dict_val: Any) -> None:
            """Function for collecting spark conf.

            Check if the input value is a dict;
            if yes, search for spark_config and collect into 'spark_conf_list' (WIP).

            Args:
                dict_val (Any): Input SparkSession.

            """
            if type(dict_val) is dict:
                if "spark_config" in dict_val.keys():
                    for sc_key in dict_val["spark_config"].keys():
                        spark_conf_list.append(sc_key)

                for ly_val in dict_val.values():
                    _collect_all_spark_conf(ly_val)

        for each_val in conf.values():
            _collect_all_spark_conf(each_val)

        for each_spark_conf in set(spark_conf_list):
            each_spark_val = spark.conf.get(each_spark_conf)
            default_conf_list.append((each_spark_conf, each_spark_val))

        return default_conf_list

    def _set_pipeline_obj(args: Tuple, kwargs: Dict) -> None:
        """Function for setting '_pipeline_obj'.

        if from_handler, takes vars from _default_obj
        else takes vars from args and kwargs.

        Args:
            args (Tuple): Input arguments.
            kwargs (Dict): Input keyword arguments.

        """
        to_pass_list = ["conf", "logger", "dbutils", "spark"]

        if _default_obj["from_handler"]:
            _pipeline_obj["default"] = {}
            for key, value in _default_obj["default"].items():
                _pipeline_obj["default"][key] = value

            _pipeline_obj["spark"] = _default_obj["spark"]
        else:
            _pipeline_var_dict = _create_pipeline_var_dict(args, kwargs)
            _set_default_vars_to_pipeline_obj(_pipeline_var_dict, to_pass_list)
        # _pipeline_obj["default_spark_conf"] = _get_default_spark_conf(_pipeline_obj["spark"], _pipeline_obj["default"]["conf"])

    def set_var_dict(self: Any) -> Dict:
        """Function for setting kwargs.

        Set self vars and create _var_dict to be passed as kwargs.

        Returns:
            Dict: A Dict to be passed as kwargs.

        """
        _var_dict = {}
        for key, value in _pipeline_obj["default"].items():
            if key == "conf":
                _var_dict["conf"] = value
                _dict_key = function.__qualname__.split(".", 1)[0]
                # there must be a pipeline section in conf
                _dict_each_class = value[_dict_key]
                for ec_key, ec_value in _dict_each_class.items():
                    if ec_key == "spark_config":
                        pass
                    # else:
                    #     setattr(self, ec_key, ec_value)
            else:
                _var_dict[key] = value
                # setattr(self, key, value)
        _var_dict["spark"] = _pipeline_obj["spark"]
        # setattr(self, "spark", _pipeline_obj["spark"])
        return _var_dict

    def pop_pipeline_var_dict(_var_dict: Dict, args: Tuple, kwargs: Dict) -> None:
        """Function to pop keys of _var_dict, dict containing vars for pipeline class

        If there are hard-coded vars of pipeline class which are called from handler,
        those hard-coded vars will take priority over the same vars from conf.
        If there are vars of pipeline class that the pipeline class does not define in the function,
        those vars will be taken out from kwargs that will be provided to pipeline class.

        Args:
            _var_dict: Input variable dict
            args (Tuple): Input arguments.
            kwargs (Dict): Input keyword arguments.

        """
        pipeline_co_var = [i for i in function.__code__.co_varnames if i != "self"]

        pop_var_dict(pipeline_co_var, _var_dict, args, kwargs)

    @functools.wraps(function)
    def wrap_set_pipeline_obj(self: Any, *args: Any, **kwargs: Any) -> None:
        """Main function of set_pipeline_obj.

        Main function for setting pipeline vars
        set from_pipeline flag and provide kwargs if run from __main__

        Args:
            self: Class passed from __init__
            *args (Any): Input arguments.
            **kwargs (Any): Input keyword arguments.

        """
        _default_obj["from_pipeline"] = True
        _set_pipeline_obj(args, kwargs)
        _var_dict = set_var_dict(self)
        _dict_key = function.__qualname__.split(".", 1)[0]

        cleaned_configs = PipelineConfigArgumentValidators(
            **_var_dict["conf"][_dict_key]
        ).dict()
        _var_dict["conf"][_dict_key] = cleaned_configs

        if _default_obj["from_handler"]:
            pop_pipeline_var_dict(_var_dict, args, kwargs)
            function(self, *args, **kwargs, **_var_dict)
        else:
            function(self, *args, **kwargs)

    return cast(F, wrap_set_pipeline_obj)


def validate_schema_path_in_cfg_endswith_dot_json(cfg_dict: Dict) -> Optional[Dict]:
    """Function for validating schema paths.

    Validate `input_schema_path` and `ref_schema_path` in a configuration dictionary
    at the key named `data_source`. The configuration dictionary can have any depth
    and only the depth level where both `input_schema_path` and `ref_schema_path` are
    found at the same time will be validated and any values that go after will be ignored
    and have their values maintained.

    Args:
        cfg_dict (Dict): Conf dict.

    Returns:
        Dict: Conf dict after validated.

    """

    schema_related_keys = ["input_schema_path", "ref_schema_path"]
    # Validate and parse (if possible) whenever `input_schema_path` and `ref_schema_path`
    # are found at the same time.
    if all([cfg_dict.get(x, False) for x in schema_related_keys]):
        cleaned_schema_paths = TransformationConfigArgumentValidator(**cfg_dict).dict()
        return cleaned_schema_paths

    # Recursively walking through each of the depth levels of the configuration dictionary
    # in a mission of searching for `input_schema_path` and `ref_schema_path`.
    for key, value in cfg_dict.items():
        if isinstance(value, dict):
            ret = validate_schema_path_in_cfg_endswith_dot_json(value)

            # If the return value is not None, then it means we got the cleaned version
            # of the values, i.e. the cleaned_schema_paths was created and validated.
            # In this case, we replace the unvalidated values with the validated ones.
            if ret is not None:
                cfg_dict[key] = ret
    return None


def set_tfm_obj(function: F) -> F:
    """Decorator function for setting transformation class objects.

    Take conf, logger, spark from _pipeline_obj (created from pipeline)
    and pass these variables to function as keyword arguments
    but if not being run from pipeline, this decorator does nothing.

    Args:
        function (Callable): A __init__ function of a pipeline class.

    Returns:
        Callable: 'wrap_set_tfm_obj' function.

    """

    def set_var_dict() -> Dict:
        """Function for setting a variable dict.

        Set variables to be pass into function as kwargs.

        Returns:
            Dict: A variable dict.

        """
        _var_dict = {}
        for key, value in _pipeline_obj["default"].items():
            if key == "conf":
                _dict_key = function.__qualname__.split(".", 1)[0]
                # if there is a tfm class conf, add it into _var_dict
                if _dict_key in value.keys():
                    _dict_each_class = value[_dict_key]
                    for ec_key, ec_value in _dict_each_class.items():
                        if ec_key == "spark_config":
                            pass
                        else:
                            _var_dict[ec_key] = ec_value
            else:
                _var_dict[key] = value
        _var_dict["spark"] = _pipeline_obj["spark"]
        return _var_dict

    def pop_tfm_var_dict(_var_dict: Dict, args: Tuple, kwargs: Dict) -> None:
        """Function to pop keys of _var_dict, dict containing vars for Tfm class

        If there are hard-coded vars of Tfm class which are called from a pipeline class,
        those hard-coded vars will take priority over the same vars from conf.
        If there are vars of Tfm class that the Tfm class does not define in the function,
        those vars will be taken out from kwargs that will be provided to Tfm class.

        Args:
            _var_dict: Input variable dict
            args (Tuple): Input arguments.
            kwargs (Dict): Input keyword arguments.

        """
        tfm_co_var = [i for i in function.__code__.co_varnames if i != "self"]

        pop_var_dict(tfm_co_var, _var_dict, args, kwargs)

    @functools.wraps(function)
    def wrap_set_tfm_obj(self: Any, *args: Any, **kwargs: Any) -> None:
        """Main function of set_tfm_obj.

        Main function for setting trnsformation vars
        Determine if executed from a pipeline or not
        if yes, set and pass variables into function

        Args:
            self: Class passed from __init__
            *args (Any): Input arguments.
            **kwargs (Any): Input keyword arguments.

        """

        data_source_key_name = (
            "data_source"  # the key that its value is required to be validated.
        )
        function_argument_list = list(
            function.__code__.co_varnames
        )  # list of all arguments of __init__() function of the decorated class.
        data_source_index = (
            function_argument_list.index(data_source_key_name) - 1
        )  # find the index of the `data_source`, minus 1 due to `args` skips `self`.
        if _default_obj["from_pipeline"]:
            # create dict to pass into function

            _var_dict = set_var_dict()  # passed
            pop_tfm_var_dict(_var_dict, args, kwargs)  # passed

            # Validate _var_dict here. The reason we ignore the return value here is because
            # within the recursive calls of the function we already have a step that replace
            # the unvalidated values with the validated ones, i.e., the line that has a code:
            # if ret is not None: cfg_dict[key] = ret.
            if data_source_key_name in _var_dict.keys():
                _ = validate_schema_path_in_cfg_endswith_dot_json(
                    _var_dict[data_source_key_name]
                )

            function(self, *args, **kwargs, **_var_dict)
        else:
            # handle the case when `data_source` is passed as a keyword argument
            if kwargs.get(data_source_key_name, None):
                _ = validate_schema_path_in_cfg_endswith_dot_json(
                    kwargs[data_source_key_name]
                )
            # handle the case when `data_source` is passed as a positional argument
            elif data_source_index < len(args) and args[data_source_index]:
                _ = validate_schema_path_in_cfg_endswith_dot_json(args[data_source_index])

            function(self, *args, **kwargs)

    return cast(F, wrap_set_tfm_obj)


def set_default_obj(func: F) -> F:
    """Decorator function for setting default objects.

    Store self.conf, self.logger, self.dbutils, self.spark in a dictionary
    and mark "from_handler" flag for being run from __main__

    Args:
        function (Callable): A __init__ function of a pipeline class.

    Returns:
        Callable: 'wrap_add_default_obj' function.

    """

    @functools.wraps(func)
    def wrap_add_default_obj(self: Any, *args: Any, **kwargs: Any) -> None:
        """Main function of set_default_obj.

        Main function for setting default obj
        put conf, logger, dbutils, spark in '_default_obj' dict and set 'from_handler' flag to True

        Args:
            self: Class passed from __init__
            *args (Any): Input arguments.
            **kwargs (Any): Input keyword arguments.

        """
        _default_obj["default"] = {
            "conf": self.conf,
            "logger": self.logger,
            "dbutils": self.dbutils,
        }
        _default_obj["spark"] = self.spark
        _default_obj["from_handler"] = True

        func(self, *args, **kwargs)

    return cast(F, wrap_add_default_obj)


if __name__ == "__main__":

    class Test:
        @set_tfm_obj
        def __init__(
            self,
            start_date: str,
            end_date: str,
            data_source: Dict,
            spark: SparkSession = None,
            logger: Any = None,
        ) -> None:
            self.spark = spark
            self.data_source = data_source
            self.logger = logger

            self.start_date = start_date
            self.end_date = end_date

    data_source = {
        "A": {"source_1": {"x": "y"}},
        "source_2": {
            "input_schema_path": "resources/people_schema.json",
            "ref_schema_path": "resources/people_schema.json",
            "input_data_endpoint": "resources/people.json",
        },
    }
    Test(
        "2022-0630", "20220640", data_source
    )  # for testing the validation flow for the case of passing data_source as a positional argument
    # Test("2022-0630", "20220640", data_source=data_source)  # for testing the validation flow for the case of passing data_source as a keyword argument
