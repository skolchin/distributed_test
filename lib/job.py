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
from types import GeneratorType, AsyncGeneratorType
from functools import cached_property
from ray.util import get_node_ip_address
from ray.exceptions import GetTimeoutError
from ray._private.worker import WORKER_MODE
from sqlmodel import SQLModel, Field, Relationship
from typing import Any, Dict, List, TypedDict, Type, ClassVar, AsyncGenerator, Tuple, cast, Callable

from lib.options import Options
from lib.json_utils import get_size, is_serializable, CustomJsonEncoder, PYDANTIC_ENCODERS

_logger = logging.getLogger(__name__)

_JobSetupT = Callable[..., Dict[str, Any] | None]
""" Job setup function """

_JobFuncT = Callable[..., Any]
""" Job execution function """

class CannotSerializeResult(Exception):
    """ Cannot serialize task results """
    pass

class InvalidResultType(Exception):
    """ Invalid result type error """
    pass

class JobRuntimeInfo(TypedDict):
    """ Job runtime information """
    ray_job_id: str
    node_id: str
    # node_name: str
    node_ip_address: str
    worker_id: str | None
    task_id: str | None
    resources: Dict[str, Any] | None

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

    def _result_to_tasks(self, options: Options, is_background: bool, result: Any) -> List['Task']:
        match result:
            case Task():
                return [result._update(is_background=is_background)]

            case Exception():
                return [Task.from_exception(self, options, is_background=is_background, ex=result)]
            
            case list() | tuple():
                results = []
                for t in result:
                    match t:
                        case Task():
                            results.append(t._update(is_background=is_background))
                        case Exception():
                            results.append(Task.from_exception(self, options, is_background=is_background, ex=t))

                return results if results else [Task.from_output(self, options, is_background=is_background, output=result)]

            case _:
                return [Task.from_output(self, options, is_background=is_background, output=result)]

    def _get_future(self, future, timeout):
        try:
            return ray.get(future, timeout=timeout)
        except Exception as e:
            _logger.error(e)
            return e

    def _get_futures(self, futures, timeout):
        # see: https://discuss.ray.io/t/handling-exceptions-from-list-of-tasks-using-ray-get/2538/5
        results = []
        ready, unready = ray.wait([f for f in futures], num_returns=1, timeout=timeout) 
        while unready:
            try:
                results.append(ray.get(ready))
            except Exception as e:
                _logger.error(e)
                results.append(e)

            ready, unready = ray.wait(unready, num_returns=1, timeout=timeout)

        return results

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
                return self._result_to_tasks(options, is_background, self._get_future(result, options.default_timeout))

            case list() | tuple() if len(result) and isinstance(result[0], ray.ObjectRef):
                return self._result_to_tasks(options, is_background, self._get_futures(result, options.default_timeout))

            case ray.ObjectRefGenerator():
                _logger.warning(f'Job {self.job_id}: job function returns generator object while running in sync mode. Use streaming mode for better performance')
                return self._result_to_tasks(options, is_background, [self._get_future(ref, options.default_timeout) for ref in result])

            case GeneratorType():
                _logger.warning(f'Job {self.job_id}: job function returns generator object while running in sync mode. Use streaming mode for better performance')
                return self._result_to_tasks(options, is_background, [ref for ref in result])

            case AsyncGeneratorType():
                raise InvalidResultType(f'Job {self.job_id}: cannot run with async generator in sync mode. Use streaming mode.')

            case _:
                return self._result_to_tasks(options, is_background, result)

    async def stream(self, 
            options: Options,
            is_background: bool = False,
            **job_kwargs,
            ) -> AsyncGenerator['Task', None]:
        """ Start a job in streaming (generator) mode """

        assert self._func
        if job_kwargs:
            self.job_kwargs = job_kwargs.copy()

        result = self._func(self, job_kwargs, options)
        match result:
            case ray.ObjectRef():
                _logger.warning(f'Job {self.job_id}: streaming is not supported by job function')
                for t in self._result_to_tasks(options, is_background, ray.get(result)):
                    yield t

            case list() | tuple() if len(result) and isinstance(result[0], ray.ObjectRef):
                _logger.warning(f'Job {self.job_id}: streaming is not supported by job function')
                for t in self._result_to_tasks(options, is_background, ray.get(cast(List[ray.ObjectRef], result))):
                    yield t

            case GeneratorType():
                _logger.info(f'Job {self.job_id}: sync streaming mode is supported')
                for ref in result:
                    yield self._result_to_tasks(options, is_background, ref)[0]

            case AsyncGeneratorType():
                _logger.info(f'Job {self.job_id}: async streaming mode is supported')
                async for ref in result:
                    yield self._result_to_tasks(options, is_background, await ref)[0]

            case ray.ObjectRefGenerator():
                _logger.info(f'Job {self.job_id}: native streaming mode is supported')
                async for ref in result:
                    yield self._result_to_tasks(options, is_background, await ref)[0]

            case _:
                _logger.warning(f'Job {self.job_id}: streaming is not supported by job function')
                for t in self._result_to_tasks(options, is_background, result):
                    yield t

    @classmethod
    def job_types(cls) -> List[JobType]:
        result = []
        pkg_name = '.'.join(__name__.split('.')[:-1]) + '.jobs'
        for fn in Path(__file__).parent.joinpath('jobs').glob('*.py'):
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

        if ctx.worker.mode == WORKER_MODE:
            rt['task_id'] = ctx.get_task_id()
            rt['resources'] = ctx.get_assigned_resources()
        else:
            rt['task_id'] = None
            rt['resources'] = {}

        return JobRuntimeInfo(**rt)

    @classmethod
    def from_output(cls,
                    parent: Job,
                    options: Options,
                    output: Any,
                    is_background: bool = False,
                    output_ref: str | None = None) -> 'Task':
        
        rt = cls.job_runtime_info()
        data, inline, ttl, ref_id = cls._serialize_output(parent, options, rt, output_ref, output)

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

    @classmethod
    def from_exception(cls,
                    parent: Job,
                    options: Options,
                    ex: Exception,
                    is_background: bool = False) -> 'Task':
        
        rt = cls.job_runtime_info()
        return cls(
            job_id=parent.job_id,
            job=parent,
            runtime_info=rt,
            is_background=is_background,
            is_inline_output=False,
            inline_output=None,
            output_ref=None,
            error_info={'code': 500, 'error': str(ex)},
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
        fallback_task_id: str | None,
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

        if not (ref_id := rt.get('task_id') or fallback_task_id):
            _logger.warning(f'Job {parent.job_id}: neither runtime task_id not fallback task_id is available, will use random UUID for results storage')
            ref_id = str(uuid4())

        if not result_storage_uri:
            raise CannotSerializeResult(f'Job {parent.job_id}: cannot serialize job results to storage as `result_storage_uri` is not set')

        _logger.info(f'Job {parent.job_id}: connecting to result backend at {result_storage_uri}')
        redis_cli = redis.Redis.from_url(result_storage_uri)

        redis_cli.set(
            name=ref_id,
            value=pickle.dumps(data),
            ex=ttl,
        )
        return None, False, ttl, ref_id

    def get_raw_output(self, options: Options) -> Any:
        if self.is_inline_output:
            return self.inline_output

        assert self.output_ref        
        if not (uri := options.result_storage_uri):
            _logger.error('Cannot retrieve storage results as `result_storage_uri` is not set')
            return None

        _logger.info(f'Connecting to result backend at {uri}')
        redis_cli = redis.Redis.from_url(uri)

        result = redis_cli.get(name=self.output_ref)
        if result is None:
            return None
        
        assert isinstance(result, bytes)
        return pickle.loads(result)

def remote_job(
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
                _logger.debug(f'Job {job.job_id}: calling setup() with {kwargs}')
                setup_kwargs = setup_func(job, options, **kwargs)
                kwargs = (kwargs or {}) | (setup_kwargs or {})

            _logger.debug(f'Job {job.job_id}: remote execution')
            has_batches = kwargs.pop('has_batches', False)
            if not supports_batches or not has_batches:
                _logger.debug(f'Starting single batch with {kwargs}')
                return run_remote.remote(job, options, **kwargs)

            if not 'data' in kwargs or not isinstance(kwargs['data'], (list, tuple, np.ndarray)):
                _logger.warning(f'Job {job.job_id}: cannot establish batched execution as data provided is not list-like')
                return run_remote.remote(job, options, **kwargs)
                
            data = kwargs.pop('data')
            _logger.debug(f'Job {job.job_id}: staring {data.shape[0]} batches with {kwargs}')
            return [run_remote.remote(job, options, **(kwargs | {'data': d})) for d in data]

        job = Job(
            job_type=job_type or 'remote',
            supports_background=supports_background,
            requirements=requirements or {},
            job_kwargs=kwargs)
        
        job._func=run_job
        return job
    
    return wrapper

def local_job(
    job_type: str | None = None,
    supports_background: bool = True,
    requirements: Dict[str, Any] | None = None,
    setup_func: _JobSetupT | None = None):

    @wrapt.decorator
    def wrapper(wrapped, instance, args, kwargs) -> Job:

        def run_job(job: Job, kwargs: Dict[str, Any], options: Options):
            if setup_func:
                _logger.debug(f'Job {job.job_id}: calling setup() with {kwargs}')
                setup_kwargs = setup_func(job, options, **kwargs)
                kwargs = (kwargs or {}) | (setup_kwargs or {})

            _logger.debug(f'Job {job.job_id}: local execution')
            return wrapped(job, options, **kwargs)
        
        job = Job(
            job_type=job_type or 'local',
            supports_background=supports_background,
            requirements=requirements or {},
            job_kwargs=kwargs)
        
        job._func=run_job
        return job
    
    return wrapper
