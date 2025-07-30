import sys
import ray
import pickle
import inspect
import logging
import importlib
import numpy as np
from uuid import uuid4
from pathlib import Path
from abc import abstractmethod
from ray._private.worker import WORKER_MODE
from pydantic import BaseModel, Field, PrivateAttr, field_serializer
from typing import Any, Dict, List, TypedDict, Type, ClassVar

_logger = logging.getLogger(__name__)

class JobTypeInfo(TypedDict):
    """ Structured job type info """
    file_name: str
    """ Name of file where job type is defined """

    job_type: str
    """ Job type name"""

    job_cls: Type['Job']
    """ Job class"""

class JobRuntimeInfo(TypedDict):
    """ Job runtime information """
    ray_job_id: str
    ray_node_id: str
    node_ip_address: str
    ray_worker_id: str | None
    ray_task_id: str | None
    resources: Dict[str, Any] | None

class JobResult(BaseModel):
    """ Job result """
    job_id: str
    job_type: str
    runtime_info: JobRuntimeInfo | None = None
    output: Any | None = Field(default=None, repr=False)
    raw_output: Any | None = Field(default=None, repr=False, exclude=True)

    @classmethod
    def from_job(cls, parent: 'Job', output: Any | None = None) -> 'JobResult':
        rt = parent.job_runtime_info()
        return cls(
            job_id=parent.job_id,
            job_type=parent.job_type,
            runtime_info=rt,
            raw_output=output,
            output=cls._serialize_output(output, node_ip=rt.get('node_ip_address')),
        )

    @classmethod
    def from_output(cls, parent: 'Job', output: Any) -> 'JobResult':
        rt = parent.job_runtime_info()
        return cls(
            job_id=parent.job_id,
            job_type=parent.job_type,
            runtime_info=rt,
            raw_output=output,
            output=cls._serialize_output(output, node_ip=rt.get('node_ip_address')),
        )

    @classmethod
    def _serialize_output(cls, data: Any, node_ip: str | None = None) -> Any:

        def get_size(x) -> int:
            match x:
                case np.ndarray():
                    return data.nbytes

                case list() | tuple():
                    return sum([get_size(v) for v in x]) + sys.getsizeof(x)
                
                case dict():
                    return sum([get_size(v) for v in x.values()]) + sys.getsizeof(x)

                case _:
                    return sys.getsizeof(x)

        def make_serializable(x):
            match x:
                case np.ndarray():
                    return x.tolist()

                case list() | tuple():
                    return [make_serializable(v) for v in x]
                
                case dict():
                    return {k: make_serializable(v) for k,v in x.items()}

                case int() | float() | bool() | str() | None:
                    return x
                
                case _:
                    raise ValueError(f'Non serializable: {type(x)}')
        
        size = get_size(data)
        if size <= 1024:
            # return as is with conversion
            try:
                return make_serializable(data)
            except ValueError:
                # non-serializable, go on with pickling
                pass
        
        # pickle to disk and return path/URL
        # TODO: security
        fn = Path('.').absolute().joinpath('www', 'data', f'{uuid4()}.pkl').relative_to(Path('.').absolute())
        with open(fn, 'wb') as fp:
            pickle.dump(data, fp)

        return str(fn) if not node_ip else f'http://{node_ip}:8000/{str(fn).replace("www/","")}'


class Job(BaseModel):
    """ Ray job """

    job_type: ClassVar[str] = ''
    """ Job type """

    job_id: str = Field(default_factory=lambda: str(uuid4()))
    """ Internal job id """

    supports_background: bool = False
    """ Shall run in background """

    requirements: Dict[str, Any] = Field(default_factory=dict)
    """ Job resource requirements """

    @staticmethod
    def job_runtime_info() -> JobRuntimeInfo:
        """ Runtime info (valid only within ray-called function) """
        rt = {}

        ctx = ray.get_runtime_context()
        rt['ray_job_id'] = ctx.get_job_id()
        rt['ray_node_id'] = ctx.get_node_id()
        rt['ray_worker_id'] = ctx.get_worker_id()
        if ctx.worker.mode == WORKER_MODE:
            rt['ray_task_id'] = ctx.get_task_id()
            rt['resources'] = ctx.get_assigned_resources()
        else:
            rt['ray_task_id'] = None
            rt['resources'] = {}

        rt['node_ip_address'] = ray._private.services.get_node_ip_address() # type:ignore

        return JobRuntimeInfo(**rt)

    def setup(self) -> Dict[str, Any] | None:
        """ Sets a job up """
        pass

    @abstractmethod
    def start(self, *args, **kwargs) -> ray.ObjectRef | List[ray.ObjectRef]:
        pass

    def run(self, *args, **kwargs) -> Any:
        futures = self.start(*args, **kwargs)
        result = ray.get(futures)
        return result

    @classmethod
    def job_types(cls) -> List[JobTypeInfo]:
        result = []
        pkg_name = '.'.join(__name__.split('.')[:-1])
        for fn in Path(__file__).parent.glob('*.py'):
            try:
                mod_name = fn.with_suffix('').name
                if mod_name in globals():
                    mod = globals()[mod_name]
                else:
                    mod = importlib.import_module('.' + mod_name, pkg_name)
                for mod_cls_name, mod_cls in inspect.getmembers(mod, inspect.isclass):
                    if mod_cls != cls and issubclass(mod_cls, cls):
                        result.append(
                            JobTypeInfo(
                                file_name=str(fn), 
                                job_type=mod_cls.job_type or mod_cls_name, 
                                job_cls=mod_cls))
                        globals()[mod_name] = mod

            except (ModuleNotFoundError, ImportError) as ex:
                print(ex)

        return result
