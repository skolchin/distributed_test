import sys
import json
import inspect
import logging
import datetime
import numpy as np
from typing import Dict, Any, Tuple, Callable, List, get_origin, get_args

_logger = logging.getLogger(__name__)

def get_size(x: Any) -> int:
    """ Calculate size of an object """
    match x:
        case np.ndarray():
            return x.nbytes

        case list() | tuple():
            return sum([get_size(v) for v in x]) + sys.getsizeof(x)
        
        case dict():
            return sum([get_size(v) for v in x.values()]) + sys.getsizeof(x)

        case _:
            return sys.getsizeof(x)

def is_serializable(x: Any, with_custom_serializers: bool = True) -> bool:
    """ Checks whether the object is serializable to JSON """
    match x:
        case np.ndarray() if with_custom_serializers:
            return True

        case datetime.datetime() | datetime.date() | datetime.time() if with_custom_serializers:
            return True
        
        case list() | tuple():
            return all([is_serializable(v, with_custom_serializers) for v in x])
        
        case dict():
            return all(is_serializable(v, with_custom_serializers) for v in x.values())

        case int() | float() | bool() | str() | None:
            return True
        
        case _:
            return False

# Cannot use json5 / orjson as Pydantic doesnt support them
# Has to run custom encoder
class CustomJsonEncoder(json.JSONEncoder):
    """ JSON encoder for numpy and datetime types"""
    def default(self, o):
        if o is None:
            return None
        if isinstance(o, np.integer):
            return int(o)
        elif isinstance(o, np.floating):
            return float(o)
        elif isinstance(o, np.ndarray):
            return o.tolist()
        if isinstance(o, (datetime.datetime, datetime.date, datetime.time)):
            return o.isoformat()
        if isinstance(o, datetime.timedelta):
            return o.total_seconds()
        return json.JSONEncoder.default(self, o)

# Pydantic could use encoding functions only
_encoder = CustomJsonEncoder()

PYDANTIC_ENCODERS = {
    datetime.datetime: lambda v: _encoder.default(v),
    datetime.date: lambda v: _encoder.default(v),
    np.integer: lambda v: _encoder.default(v),
    np.floating: lambda v: _encoder.default(v),
    np.ndarray: lambda v: _encoder.default(v),
}

def sqla_json_serializer(o):
    return json.dumps(o, cls=CustomJsonEncoder, ensure_ascii=False)

def sqla_json_deserializer(o):
    return json.loads(o)

def cast_to_signature(data: Dict[str, Any], func: Callable) -> Dict[str, Any]:
    annot = inspect.getfullargspec(func).annotations

    result = {}
    for var, val in data.items():
        if (cast_type := annot.get(var)) is None:
            result[var] = val
            continue

        if cast_type == Any:
            result[var] = val
            continue

        if (type_orig := get_origin(cast_type)):
            cast_type = type_orig

        if cast_type == np.ndarray:
            result[var] = val
            continue

        if cast_type:
            try:
                result[var] = cast_type(val)
                if result[var]:
                    if cast_type in (list, tuple):
                        if not isinstance(result[var], (list, tuple)):
                            _logger.error(f'Error casting argument {var} to type {cast_type}: source value type is incompatible ({type(result[var])})')
                        else:
                            cast_subtype = get_args(annot[var])[0]
                            if cast_subtype != Any:
                                result[var] = [cast_subtype(v) for v in result[var]]
                    elif cast_type == dict:
                        if not isinstance(result[var], dict):
                            _logger.error(f'Error casting argument {var} to type {cast_type}: source value type is incompatible ({type(result[var])})')
                        else:
                            if len(cast_subtypes := get_args(annot[var])) == 2 and cast_subtypes[1] != Any:
                                result[var] = {cast_subtypes[0](k): cast_subtypes[1](v) for k,v in result[var].items()}

            except Exception as e:
                _logger.error(f'Error casting argument {var} to type {cast_type}: {e}')

    return result
