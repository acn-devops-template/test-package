"""decorator init_auto_parameters, parse_auto_parameters, get_auto_parameters modules"""

# import: standard
import functools
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import Tuple
from typing import TypeVar
from typing import cast

# func type annotation
F = TypeVar("F", bound=Callable[..., Any])

# dict to store obj created from __main__
_default_obj: Dict = {"from_handler": False, "from_pipeline": False}
# dict to store obj created from a pipeline
_pipeline_obj: Dict = {}


def get_co_var_names_without_self(function: F) -> List:
    """Function to get function's variable names not including "self".

    Args:
        function (Callable): A function.

    Returns:
        List: A list of function's variable names.

    """
    co_var_names_wo_self = [i for i in function.__code__.co_varnames if i != "self"]
    return co_var_names_wo_self


def pop_function_var_dict(
    function: F, _var_dict: Dict, args: Tuple, kwargs: Dict
) -> None:
    """Function to pop keys of _var_dict, dict containing vars.

    If there are hard-coded vars which are called from handler,
    those hard-coded vars will take priority over the same vars from conf.
    If there are vars that the class does not define in the function,
    those vars will be taken out from kwargs.

    Args:
        _var_dict: Input variable dict
        args (Tuple): Input arguments.
        kwargs (Dict): Input keyword arguments.

    """

    fn_co_var = get_co_var_names_without_self(function)
    pop_var_dict(fn_co_var, _var_dict, args, kwargs)


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
    # variables in class overwrite config variables
    if len(kwargs) > 0:
        kw_common_keys = set(kwargs).intersection(_var_dict)
        for key in kw_common_keys:
            _var_dict.pop(key, None)
    if len(args) > 0:
        args_name = co_var[0 : len(args)]
        arg_common_keys = set(args_name).intersection(_var_dict)
        for key in arg_common_keys:
            _var_dict.pop(key, None)

    # pop if class doesn't define these vars, e.g. 'spark','logger','dbutils'.
    extra_keys = set(_var_dict).difference(co_var)
    for key in extra_keys:
        _var_dict.pop(key, None)


def parse_auto_parameters(function: F) -> F:
    """Decorator function for setting pipeline parameters.

    Take conf, logger, spark from _default_obj (created from __main__)
    and pass these variables to function as keyword arguments
    but if not being run from pipeline, this decorator does nothing.

    Args:
        function (Callable): A __init__ function of a pipeline class.

    Returns:
        Callable: 'wrap_parse_auto_parameters' function.

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

        _pipeline_var_list = get_co_var_names_without_self(function)
        _pipeline_var_dict = {
            _pipeline_var_list[i]: value for i, value in enumerate(args)
        }
        _pipeline_var_dict.update(kwargs)
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
        _pl_obj_dict = {
            var: _pipeline_var_dict[var] for var in var_list if var in _pipeline_var_dict
        }

        _pl_spark_ss = _pl_obj_dict.pop("spark", None)
        _pipeline_obj["default"] = _pl_obj_dict
        _pipeline_obj["spark"] = _pl_spark_ss

    def _parse_auto_parameters(args: Tuple, kwargs: Dict) -> None:
        """Function for setting '_pipeline_obj'.

        if from_handler, takes vars from _default_obj
        else takes vars from args and kwargs.

        Args:
            args (Tuple): Input arguments.
            kwargs (Dict): Input keyword arguments.

        """
        to_pass_list = ["conf", "logger", "dbutils", "spark"]

        if _default_obj["from_handler"]:
            _pipeline_obj.update(_default_obj)
        else:
            _pipeline_var_dict = _create_pipeline_var_dict(args, kwargs)
            _set_default_vars_to_pipeline_obj(_pipeline_var_dict, to_pass_list)

    def set_var_dict() -> Dict:
        """Function for setting kwargs.

        Set _var_dict to be passed as kwargs.

        Returns:
            Dict: A Dict to be passed as kwargs.

        """
        _var_dict = {key: value for key, value in _pipeline_obj["default"].items()}
        _var_dict["spark"] = _pipeline_obj.get("spark")
        return _var_dict

    @functools.wraps(function)
    def wrap_parse_auto_parameters(self: Any, *args: Any, **kwargs: Any) -> None:
        """Main function of parse_auto_parameters.

        Main function for setting pipeline parameters
        set from_pipeline flag and provide kwargs if run from __main__

        Args:
            self: Class passed from __init__
            *args (Any): Input arguments.
            **kwargs (Any): Input keyword arguments.

        """
        _default_obj["from_pipeline"] = True
        _parse_auto_parameters(args, kwargs)
        _var_dict = set_var_dict()

        if _default_obj["from_handler"]:
            pop_function_var_dict(function, _var_dict, args, kwargs)
            function(self, *args, **kwargs, **_var_dict)
        else:
            function(self, *args, **kwargs)

    return cast(F, wrap_parse_auto_parameters)


def get_auto_parameters(function: F) -> F:
    """Decorator function for setting transformation class parameters.

    Take conf, logger, spark from _pipeline_obj (created from pipeline)
    and pass these variables to function as keyword arguments
    but if not being run from pipeline, this decorator does nothing.

    Args:
        function (Callable): A __init__ function of a pipeline class.

    Returns:
        Callable: 'wrap_get_auto_parameters' function.

    """

    def set_var_dict() -> Dict:
        """Function for setting a variable dict.

        Set variables to be pass into function as kwargs.

        Returns:
            Dict: A variable dict.

        """
        _var_dict = {}
        default_vars = _pipeline_obj["default"]
        class_name = function.__qualname__.split(".", 1)[0]

        for key, value in default_vars.items():
            if key == "conf" and class_name in value:
                _var_dict.update(value[class_name])
            else:
                _var_dict[key] = value
        _var_dict["spark"] = _pipeline_obj["spark"]
        return _var_dict

    @functools.wraps(function)
    def wrap_get_auto_parameters(self: Any, *args: Any, **kwargs: Any) -> None:
        """Main function of get_auto_parameters.

        Main function for setting trnsformation vars
        Determine if executed from a pipeline or not
        if yes, set and pass variables into function

        Args:
            self: Class passed from __init__
            *args (Any): Input arguments.
            **kwargs (Any): Input keyword arguments.

        """

        if _default_obj["from_pipeline"]:
            # create dict to pass into function
            _var_dict = set_var_dict()
            pop_function_var_dict(function, _var_dict, args, kwargs)
            function(self, *args, **kwargs, **_var_dict)
        else:
            # if not being run from pipeline, do nothing
            function(self, *args, **kwargs)

    return cast(F, wrap_get_auto_parameters)


def init_auto_parameters(func: F) -> F:
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
        """Main function of init_auto_parameters.

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
