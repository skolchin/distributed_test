import ray
import wrapt
import redis
import pickle
import inspect
import logging
import importlib
import numpy as np
from uuid import uuid4
from pathlib import Path
from sqlalchemy import JSON
from datetime import datetime
from functools import cached_property
from ray.util import get_node_ip_address
from ray._private.worker import WORKER_MODE
from sqlmodel import SQLModel, Field, Relationship
from typing import Any, Dict, List, TypedDict, Type, ClassVar, Generator, Tuple, cast, Callable

from lib.options import Options
from lib.job.types import JobRuntimeInfo
from lib.json_utils import get_size, is_serializable, CustomJsonEncoder, PYDANTIC_ENCODERS

_logger = logging.getLogger(__name__)

_JobSetupT = Callable[..., Dict[str, Any] | None]
""" Job setup function """

_JobFuncT = Callable[..., Any]
""" Job execution function """

class CannotSerializeResult(Exception):
    """ Cannot serialize results exception """
    pass

class JobType(TypedDict):
    """ Structured job type info """
    file_name: str
    """ Name of file where job type is defined """

    job_cls: Type['Job'] | None
    """ Job class reference (for class-based jobs) """

    job_func: Callable[..., 'Job'] | None
    """ Job function reference (for functions decorated with `job`) """

class ErrorInfo(TypedDict):
    """ Structured error info """
    code: int
    """ Error code """

    error: str | None
    """ Error message """

class JobBase(SQLModel):
    """ Job """

    job_id: str = Field(default_factory=lambda: str(uuid4()), primary_key=True)
    """ Job id """

    job_type: str = Field(index=True, nullable=False)
    """ Instance-level job type """

    job_kwargs: Dict[str, Any] = Field(default_factory=dict, nullable=False, sa_type=JSON)
    """ Job run arguments """

    requirements: Dict[str, Any] = Field(default_factory=dict, nullable=False, sa_type=JSON)
    """ Task requirements """

    supports_background: bool = Field(default=True, nullable=False)
    """ Background support """


class Job(JobBase, table=True):
    """ Job """

    default_job_type: ClassVar[str] = 'default'
    """ Default job type, has to be overridden for descendant classes """

    tasks: list['Task'] = Relationship(back_populates='job', sa_relationship_kwargs={"lazy": "subquery"})
    """ List of job tasks """

    model_config = {
        'arbitrary_types_allowed': True,
    } # type:ignore

    _func: _JobFuncT | None = None
    """ Function to be executed (private) """

    def run(self, 
            options: Options,
            is_background: bool = False,
            **job_kwargs,
            ) -> List['Task']:
        """ Start a job and wait for result """

        assert self._func
        if job_kwargs:
            self.job_kwargs = job_kwargs.copy()

        result = self._func(self, job_kwargs, options)
        match result:
            case ray.ObjectRef():
                result = ray.get(result)

            case list() | tuple() if len(result) and isinstance(result[0], ray.ObjectRef):
                result = ray.get(cast(List[ray.ObjectRef], result))

        match result:
            case Task():
                return [result._update(is_background=is_background)]

            case list() | tuple() if len(result) and isinstance(result[0], Task):
                return list([cast(Task, r)._update(is_background=is_background) for r in result])
            
            case _:
                return [Task.from_output(self, options, is_background=is_background, output=result)]

    async def stream(self, 
            options: Options,
            is_background: bool = False,
            **job_kwargs,
            ):
        """ Start a job in streaming mode """
        result = self.run(options, is_background, **job_kwargs)
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

                # check for classes inherited from Job
                for mod_cls_name, mod_cls in inspect.getmembers(mod, inspect.isclass):
                    if mod_cls != cls and issubclass(mod_cls, cls):
                        result.append(
                            JobType(
                                file_name=str(fn), 
                                job_cls=mod_cls,
                                job_func=None,
                            ))
                        globals()[mod_name + '.' + mod_cls_name] = mod

                # check for `job`-decorated functions
                for mod_func_name, mod_func in inspect.getmembers(mod, inspect.isfunction):
                    if isinstance(mod_func, wrapt.FunctionWrapper):
                        result.append(
                            JobType(
                                file_name=str(fn), 
                                job_cls=None,
                                job_func=mod_func,
                            ))
                        globals()[mod_name + '.' + mod_func_name] = mod


            except (ModuleNotFoundError, ImportError) as ex:
                _logger.error(f'Error importing {fn}: {ex}')

        return result

class TaskBase(SQLModel):
    """ Task SQL base """

    task_id: int | None = Field(default=None, primary_key=True)
    """ Task id """

    job_id: str = Field(foreign_key='job.job_id')
    """ FK to parent table """

    started: datetime = Field(default_factory=datetime.now, nullable=False)
    """ datetime when job was started """

    finished: datetime | None = Field(default=None)
    """ datetime when job was finished or None if is not """

    error_info: ErrorInfo = Field(default_factory=lambda : dict(code=0), nullable=False, sa_type=JSON)
    """ Error info """

    runtime_info: JobRuntimeInfo | None = Field(default=None, sa_type=JSON)
    """ Runtime information """

    is_background: bool = Field(default=False, nullable=False)
    """" True if job instance was running in background """

    is_inline_output: bool = Field(default=True, nullable=False)
    """ True if output was stored in the instance itself """

    output_ttl: int = Field(default=0, nullable=False)
    """ Time to keep result in results storage """

    inline_output: Any | None = Field(default=None, repr=False, sa_type=JSON)
    """ Size-limited inline output (None if results were stored on results storage) """

    output_ref: str | None = Field(default=None)
    """ Result reference ID (None if results are inline) """

class Task(TaskBase, table=True):
    """ Task """

    job: Job = Relationship(back_populates='tasks')
    """ Parent job """

    model_config = {
        'json_encoders': PYDANTIC_ENCODERS,
    } # type:ignore

    _raw_output: Any | None = None
    """ Raw ouput (private) """

    @classmethod
    def job_runtime_info(cls) -> JobRuntimeInfo:
        """ Runtime info """
        rt = {}
        ctx = ray.get_runtime_context()
        rt['ray_job_id'] = ctx.get_job_id()
        rt['node_id'] = ctx.get_node_id()
        rt['worker_id'] = ctx.get_worker_id()
        rt['node_ip_address'] = get_node_ip_address()

        _logger.debug(f'Worker is in {ctx.worker.mode} mode')
        if ctx.worker.mode == WORKER_MODE:
            rt['task_id'] = ctx.get_task_id()
            rt['resources'] = ctx.get_assigned_resources()
        else:
            rt['task_id'] = None
            rt['resources'] = {}

        _logger.debug(f'Runtime info: {rt}')
        return JobRuntimeInfo(**rt)

    @classmethod
    def from_output(cls,
                    parent: Job,
                    options: Options,
                    output: Any,
                    is_background: bool=False) -> 'Task':
        rt = cls.job_runtime_info()
        data, inline, ttl, ref_id = cls._serialize_output(parent, options, rt, output)
        return cls(
            job_id=parent.job_id,
            job=parent,
            runtime_info=rt,
            is_inline_output=inline,
            is_background=is_background,
            output_ttl=ttl,
            inline_output=data,
            output_ref=ref_id,
        )

    def _update(self, is_background: bool) -> 'Task':
        self.is_background = is_background
        return self

    @classmethod
    def _serialize_output(
        cls,
        parent: Job,
        options: Options,
        rt: JobRuntimeInfo,
        data: Any) -> Tuple[Any, bool, int, str | None]:

        max_inline_result_size = options.max_inline_result_size or 0
        result_storage_uri = options.result_storage_uri
        ttl = options.default_result_ttl or 30*60

        if data is None:
            return None, True, 0, None

        return_inline = True
        sz = get_size(data)
        if max_inline_result_size and sz > max_inline_result_size:
            return_inline = False

        if return_inline and not is_serializable(data):
            return_inline = False

        if return_inline:
            return data, True, 0, None

        if not (ref_id := rt.get('task_id')):
            raise CannotSerializeResult(f'Task ID is empty')

        if not result_storage_uri:
            raise CannotSerializeResult('Cannot serialize results to storage as `result_storage_uri` is not set')

        _logger.info(f'Connecting to result backend at {result_storage_uri}')
        redis_cli = redis.Redis.from_url(result_storage_uri)

        redis_cli.set(
            name=ref_id,
            value=pickle.dumps(data),
            ex=ttl,
        )
        return None, False, ttl, ref_id

    @cached_property
    def raw_output(self) -> Any:
        if self.is_inline_output:
            return self.inline_output
        
        if not (uri := self.runtime_info.get('result_storage_uri') if self.runtime_info else None):
            _logger.error('Cannot retrieve storage results as `result_storage_uri` is not set')
            return None

        _logger.info(f'Connecting to result backend at {uri}')
        redis_cli = redis.Redis.from_url(uri)

        result = redis_cli.get(name=(self.runtime_info or {}).get('task_id') or self.job.job_id)
        if result is None:
            return None
        
        assert isinstance(result, bytes)
        return pickle.loads(result)

    @cached_property
    def full_output(self) -> Any:
        output = self.raw_output
        return CustomJsonEncoder(ensure_ascii=False).default(output)

def job(
    job_type: str | None = None,
    supports_background: bool = True,
    supports_batches: bool = True,
    requirements: Dict[str, Any] | None = None,
    ray_kwargs: Dict[str, Any] | None = None,
    setup_func: _JobSetupT | None = None):

    @wrapt.decorator
    def wrapper(wrapped, instance, args, kwargs) -> Job:

        remote_kwargs = (requirements or {}) | (ray_kwargs or {})
        if not 'scheduling_strategy' in remote_kwargs:
            remote_kwargs['scheduling_strategy'] = 'SPREAD'

        @ray.remote(**remote_kwargs)
        def run_remote(job: Job, options: Options, **kwargs) -> None | ray.ObjectRef  | List[ray.ObjectRef]:
            return wrapped(job, options, **kwargs)

        def run_job(job: Job, kwargs: Dict[str, Any], options: Options):
            if setup_func:
                _logger.debug(f'Calling setup() with {kwargs}')
                setup_kwargs = setup_func(job, options, **kwargs)
                kwargs = (kwargs or {}) | (setup_kwargs or {})

            has_batches = kwargs.pop('has_batches', False)
            if not supports_batches or not has_batches:
                _logger.debug(f'Starting single batch with {kwargs}')
                return run_remote.remote(job, options, **kwargs)

            if not 'data' in kwargs or not isinstance(kwargs['data'], (list, tuple, np.ndarray)):
                _logger.warning('Cannot establish batched execution as setup results are not list-like')
                return run_remote.remote(job, options, **kwargs)
            
            data = kwargs.pop('data')
            _logger.debug(f'Staring {data.shape[0]} batches with {kwargs}')
            return [run_remote.remote(job, options, **(kwargs | {'data': d})) for d in data]

        job = Job(
            job_type=job_type or 'test',
            supports_background=supports_background,
            requirements=requirements or {},
            job_kwargs=kwargs)
        
        job._func=run_job
        return job
    
    return wrapper
