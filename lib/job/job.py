import ray
import redis
import pickle
import inspect
import logging
import importlib
from uuid import uuid4
from pathlib import Path
from numpy.typing import NDArray
from datetime import datetime, date
from sqlalchemy import Column, JSON
from ray.util import get_node_ip_address
from ray._private.worker import WORKER_MODE
from sqlmodel import SQLModel, Field, Relationship
from typing import Any, Dict, List, TypedDict, Type, ClassVar, Generator, Tuple, Union

from lib.options import BackendOptions
from lib.job.types import JobRuntimeInfo
from lib.json_utils import get_size, is_serializable, PYDANTIC_ENCODERS

_logger = logging.getLogger(__name__)

class CannotSerializeResult(Exception):
    """ Cannot serialize results exception """
    pass

class JobType(TypedDict):
    """ Structured job type info """
    file_name: str
    """ Name of file where job type is defined """

    job_type: str
    """ Job type name"""

    job_cls: Type['Job']
    """ Job class"""

JobResultT = Union[None, NDArray, 'JobInstance']
""" Type of job run results """

class Job(SQLModel, table=True):
    """ Job information """

    cls_job_type: ClassVar[str] = ''
    """ Class-level job type. Must be overriden by descendant """

    cls_supports_background: ClassVar[bool] = True
    """ Class-level background support flag """

    job_id: str = Field(default_factory=lambda: str(uuid4()), primary_key=True)
    """ Job id """

    job_type: str = Field(index=True)
    """ Instance-level job type """

    requirements: Dict[str, Any] = Field(default_factory=dict)
    """ Job resource requirements """

    job_kwargs: Dict[str, Any] = Field(default_factory=dict)
    """ Job run arguments """

    instances: List['JobInstance'] = Relationship(back_populates='job')
    """ List of job instances """

    _options: BackendOptions | None = None
    """ Service options (private) """

    def __init__(self, options: BackendOptions | None = None):
        super().__init__(
            job_type=self.cls_job_type
        )
        self._options = options

    def setup(self) -> Dict[str, Any] | None:
        """ Sets a job up """
        pass

    def start(self, input: Dict[str, Any] | None) -> ray.ObjectRef | List[ray.ObjectRef] | None:
        """ Start job with no wait for result """
        return None

    def run(self, input: Dict[str, Any] | None) -> JobResultT | List[JobResultT]:
        """ Start job and wait for result """
        futures = self.start(input)
        if futures is None:
            return None
        result = ray.get(futures)
        return result

    def stream(self, input: Dict[str, Any] | None) -> Generator[JobResultT | List[JobResultT], None, None]:
        """ Start job in streaming mode """
        futures = self.start(input)
        if futures is None:
            return None
        result = ray.get(futures)
        yield result

    @classmethod
    def job_types(cls) -> List[JobType]:
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
                            JobType(
                                file_name=str(fn), 
                                job_type=mod_cls.cls_job_type, 
                                job_cls=mod_cls))
                        globals()[mod_name] = mod

            except (ModuleNotFoundError, ImportError) as ex:
                print(ex)

        return result

    # @model_serializer()
    # def serialize_model(self):
    #     return {
    #         'job_type': self.job_type,
    #         'job_id': self.job_id,
    #         'supports_background': self.supports_background,
    #         'requirements': self.requirements,
    #     }

class JobInstance(SQLModel, table=True):
    """ Job instance and output """

    instance_id: int | None = Field(default=None, primary_key=True)
    """ Job instance id """

    job_id: str = Field(foreign_key='job.job_id')
    """ FK to parent table """

    job: Job = Relationship(back_populates='jobinstance')
    """ Parent job """

    started: datetime = Field(default_factory=datetime.now)
    """ datetime when job was started """

    finished: datetime | None = Field(default=None)
    """ datetime when job was finished or None if is not """

    is_error: bool = Field(default=False)
    """ True, if job finished with error """

    runtime_info: JobRuntimeInfo | None = Field(default=None, sa_column=Column(JSON))
    """ Runtime information """

    is_background: bool = Field(default=False)
    """" True, if job instance was running in background """

    is_inline_output: bool = Field(default=True)
    """ True if output was stored in the instance itself """

    output_ttl: int = Field(default=0)
    """ Time to keep result in results storage """

    output: Any | None = Field(default=None, repr=False, sa_column=Column(JSON))
    """ Size-limited output (None if results were stored on results storage) """

    _raw_output: Any | None = None
    """ Raw ouput (private) """

    model_config = {
        'json_encoders': PYDANTIC_ENCODERS,
    } # type:ignore

    @classmethod
    def job_runtime_info(cls) -> JobRuntimeInfo:
        """ Runtime info """
        rt = {}
        ctx = ray.get_runtime_context()
        rt['ray_job_id'] = ctx.get_job_id()
        rt['node_id'] = ctx.get_node_id()
        rt['worker_id'] = ctx.get_worker_id()
        rt['node_ip_address'] = get_node_ip_address()

        if ctx.worker.mode == WORKER_MODE:
            rt['task_id'] = ctx.get_task_id()
            rt['resources'] = ctx.get_assigned_resources()
        else:
            rt['task_id'] = None
            rt['resources'] = {}

        return JobRuntimeInfo(**rt)

    @classmethod
    def from_job(cls,
                 parent: Job,
                 job_kwargs: Dict[str, Any] | None = None,
                 output: Any | None = None) -> 'JobInstance':

        rt = cls.job_runtime_info()
        data, inline, ttl = cls._serialize_output(rt['task_id'] or parent.job_id, output, options=parent._options)
        return cls(
            job_id=parent.job_id,
            job=parent,
            runtime_info=rt,
            _raw_output=output,
            is_inline_output=inline,
            output_ttl=ttl,
            output=data,
        )

    @classmethod
    def from_output(cls,
                    parent: Job,
                    job_kwargs: Dict[str, Any],
                    output: Any) -> 'JobInstance':
        rt = cls.job_runtime_info()
        data, inline, ttl = cls._serialize_output(rt['task_id'] or parent.job_id, output, options=parent._options)
        return cls(
            job_id=parent.job_id,
            job=parent,
            runtime_info=rt,
            is_inline_output=inline,
            output_ttl=ttl,
            output=data,
        )

    @classmethod
    def _serialize_output(
        cls,
        id: str,
        data: Any,
        options: BackendOptions | None = None) -> Tuple[Any, bool, int]:

        max_inline_result_size = options.max_inline_result_size if options else 0
        result_storage_uri = options.result_storage_uri if options else None

        if data is None:
            return None, True, 0

        return_inline = True
        sz = get_size(data)
        if max_inline_result_size and sz > max_inline_result_size:
            return_inline = False

        if return_inline and not is_serializable(data):
            return_inline = False

        if return_inline:
            return data, True, 0

        if not result_storage_uri:
            raise CannotSerializeResult('Cannot serialize results to storage as `result_storage_uri` is not set')

        _logger.info(f'Connecting to result backend at {result_storage_uri}')
        redis_cli = redis.Redis.from_url(options.result_storage_uri)    #type:ignore
        ttl = options.default_result_ttl if options else 30*60

        redis_cli.set(
            name=id,
            value=pickle.dumps(data),
            ex=ttl,
        )
        return None, False, ttl

    def get_output(self, options: BackendOptions) -> bytes | None:
        if self.output or not options.result_storage_uri:
            return pickle.dumps(self.output)
        
        _logger.info(f'Connecting to result backend at {options.result_storage_uri}')
        redis_cli = redis.Redis.from_url(options.result_storage_uri)    # type:ignore
        result = redis_cli.get(name=(self.runtime_info or {}).get('task_id') or self.job.job_id)
        if result is None:
            return None
        
        assert isinstance(result, bytes)
        return result
