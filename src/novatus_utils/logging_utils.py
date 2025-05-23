"""Reusable logging utils."""

import inspect
import logging
import time
from functools import wraps
from logging import Formatter, Handler
from typing import Callable, List, ParamSpec, TypeVar

DEFAULT_LOGGING_LEVEL = logging.INFO
DEFAULT_HANDLER = logging.StreamHandler()

logger = logging.getLogger(__name__)

_P = ParamSpec("_P")
_T = TypeVar("_T")


def setup_logging(
    formatter: Formatter | None = None,
    handler: Handler = DEFAULT_HANDLER,
    logging_level=DEFAULT_LOGGING_LEVEL,
):
    """Sets up root logger -
    Create children at module level via
    module_level_logger = logging.getLogger(__name__).

    """
    root_logger = logging.getLogger()
    root_logger.setLevel(logging_level)
    if formatter is None:
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(funcName)s:%(lineno)s - %(message)s",
            "%Y-%m-%d %H:%M:%S",
        )
    handler.setFormatter(formatter)
    root_logger.addHandler(handler)


def time_and_log(
    f: Callable[_P, _T] | None = None,
    /,
    *,
    args_to_log: List[str] | None = None,
) -> Callable[_P, _T] | Callable[[Callable[_P, _T]], Callable[_P, _T]]:
    """Decorator to time and log callables/functions."""
    args_to_log = args_to_log or None

    def run(func):
        @wraps(func)
        def wrapper(*args: _P.args, **kwargs: _P.kwargs) -> _T:
            logger.info(f"Executing {func.__module__}.{func.__name__}")
            if args_to_log:
                specified_kwargs = {key: val for key, val in kwargs.items() if key in args_to_log}
                default_kwargs = {
                    def_kwargs: inspect.signature(func).parameters[def_kwargs].default
                    for def_kwargs in inspect.signature(func).parameters
                }
                pos_args_dict = dict(zip(inspect.signature(func).parameters, args))
                all_args = (
                    default_kwargs | pos_args_dict | specified_kwargs
                )  # Order important as later values override earlier
                logged_args = [key + "=" + str(val) for key, val in all_args.items() if key in args_to_log]
                logger.info(f" {func.__module__}.{func.__name__} - arguments: {','.join(logged_args)}")
            start_time = time.perf_counter()
            result = func(*args, **kwargs)
            end_time = time.perf_counter()
            execution_time = end_time - start_time
            logger.info(f"Finished executing {func.__module__}.{func.__name__} in {execution_time:.4f} secs")
            return result

        return wrapper

    return run(f) if f else run
