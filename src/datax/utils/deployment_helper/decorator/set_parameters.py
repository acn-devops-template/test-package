# import: standard
import functools
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import Sequence
from typing import Tuple
from typing import TypeVar
from typing import cast

# func type annotation
F = TypeVar("F", bound=Callable[..., Any])

# dict to store obj created from __main__
_default_obj: Dict = {"from_handler": False, "from_pipeline": False}
# dict to store obj created from a pipeline
_pipeline_obj: Dict = {}


def set_pipeline_obj(function: F) -> F:
    """
    Take conf, logger, spark from _default_obj (created from __main__)
    and pass these variables to function as keyword arguments
    but if not being run from pipeline, this decorator does nothing.
    """

    def _create_pipeline_var_dict(args: Tuple, kwargs: Dict) -> Dict:
        """
        Create a dict that contains key:value of all variables
        that were passed into __init__ by users
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

    def _set_default_vars_to_pipeline_obj(_pipeline_var_dict: Dict, var_list: List) -> None:
        """
        Assign vars in ['conf', 'logger', 'dbutils', 'spark'] to _pipeline_obj
        """
        _pl_obj_dict = {}
        for each_obj in var_list:
            if each_obj in _pipeline_var_dict.keys():
                _pl_obj_dict[each_obj] = _pipeline_var_dict[each_obj]

        _pl_spark_ss = _pl_obj_dict.pop("spark", None)
        _pipeline_obj["default"] = _pl_obj_dict
        _pipeline_obj["spark"] = _pl_spark_ss

    def _get_default_spark_conf(spark: Any, conf: Dict) -> Sequence[Tuple[Any, Any]]:
        """
        Reserved for changing spark conf on the fly (WIP)
        """
        spark_conf_list = []
        default_conf_list = []

        def _collect_all_spark_conf(dict_val: Any) -> None:
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

    def _set_pipeline_obj(*args: Any, **kwargs: Any) -> None:
        """
        if from_handler, takes vars from _default_obj
        else takes vars from args and kwargs
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
        """
        Set self vars and create _var_dict to be passed as kwargs
        """
        _var_dict = {}
        for key, value in _pipeline_obj["default"].items():
            if key == "conf":
                _dict_key = function.__qualname__.split(".", 1)[0]
                _dict_each_class = value[_dict_key]
                _var_dict["conf"] = _dict_each_class
                for ec_key, ec_value in _dict_each_class.items():
                    if ec_key == "spark_config":
                        pass
                    else:
                        # _var_dict[ec_key] = ec_value
                        setattr(self, ec_key, ec_value)
            else:
                _var_dict[key] = value
                setattr(self, key, value)
        _var_dict["spark"] = _pipeline_obj["spark"]
        setattr(self, "spark", _pipeline_obj["spark"])
        return _var_dict

    @functools.wraps(function)
    def wrap_set_pipeline_obj(self: Any, *args: Any, **kwargs: Any) -> None:
        """
        Main function for setting pipeline vars
        set from_pipeline flag and provide kwargs if run from __main__
        """
        _default_obj["from_pipeline"] = True
        _set_pipeline_obj(*args, **kwargs)
        _var_dict = set_var_dict(self)

        if _default_obj["from_handler"]:
            function(self, *args, **kwargs, **_var_dict)
        else:
            function(self, *args, **kwargs)

    return cast(F, wrap_set_pipeline_obj)


def set_tfm_obj(function: F) -> F:
    """
    Take conf, logger, spark from _pipeline_obj (created from pipeline)
    and pass these variables to function as keyword arguments
    but if not being run from pipeline, this decorator does nothing.
    """

    def set_var_dict() -> Dict:
        """
        Set variables to be pass into function
        """
        _var_dict = {}
        for key, value in _pipeline_obj["default"].items():
            if key == "conf":
                _dict_key = function.__qualname__.split(".", 1)[0]
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

    @functools.wraps(function)
    def wrap_set_tfm_obj(self: Any, *args: Any, **kwargs: Any) -> None:
        """
        Determine if executed from a pipeline or not
        if yes, set and pass variables into function
        """
        if _default_obj["from_pipeline"]:
            # create dict to pass into function
            _var_dict = set_var_dict()
            # variables in tfm class overwrite config variables
            if len(kwargs) > 0:
                for kw_key in kwargs.keys():
                    if kw_key in _var_dict.keys():
                        _var_dict.pop(kw_key, None)
            if len(args) > 0:
                args_name = [i for i in list(function.__code__.co_varnames)[0 : len(args) + 1] if i != "self"]
                for ar_key in args_name:
                    if ar_key in _var_dict.keys():
                        _var_dict.pop(ar_key, None)
            function(self, *args, **kwargs, **_var_dict)
        else:
            function(self, *args, **kwargs)

    return cast(F, wrap_set_tfm_obj)


def set_default_obj(func: F) -> F:
    """
    Store self.conf, self.logger, self.dbutils, self.spark in a dictionary
    and mark "from_handler" flag for being run from __main__
    """

    @functools.wraps(func)
    def wrap_add_default_obj(self: Any, *args: Any, **kwargs: Any) -> None:
        _default_obj["default"] = {
            "conf": self.conf,
            "logger": self.logger,
            "dbutils": self.dbutils,
        }
        _default_obj["spark"] = self.spark
        _default_obj["from_handler"] = True

        func(self, *args, **kwargs)

    return cast(F, wrap_add_default_obj)
