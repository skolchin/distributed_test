import sys
import json
import datetime
import numpy as np
from typing import Any

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