import io
import ray
import redis
import pickle
import logging
from uuid import uuid4
from datetime import datetime, date
from sqlalchemy import Column, JSON
from sqlmodel import SQLModel, Field
from ray.util import get_node_ip_address
from ray._private.worker import WORKER_MODE
from pydantic import PrivateAttr
from typing import Any, Dict, Tuple

from lib.job.job import Job
from lib.options import BackendOptions
from lib.job.types import JobRuntimeInfo
from lib.json_utils import get_size, is_serializable, PYDANTIC_ENCODERS

_logger = logging.getLogger(__name__)

class CannotSerializeResult(Exception):
    """ Cannot serialize results exception """
    pass

class JobInstance(SQLModel, table=True):
    """ Job instance """

    instance_id: int | None = Field(default=None, primary_key=True)
    job_id: str = Field(index=True)
    job_type: str = Field(index=True)
    started: datetime = Field(default_factory=datetime.now)
    finished: datetime | None = Field(default=None)
    runtime_info: JobRuntimeInfo | None = Field(default=None, sa_column=Column(JSON))
    is_background: bool = Field(default=False)
    job_kwargs: Dict[str, Any] | None = Field(default=None, sa_column=Column(JSON))
    is_inline_output: bool = Field(default=True)
    output_ttl: int = Field(default=0)
    output: Any | None = Field(default=None, repr=False, sa_column=Column(JSON))
    _raw_output: Any | None = PrivateAttr(default=None)

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
            job_type=parent.job_type,
            runtime_info=rt,
            job_kwargs=job_kwargs,
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
            job_type=parent.job_type,
            runtime_info=rt,
            job_kwargs=job_kwargs,
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
        result = redis_cli.get(name=(self.runtime_info or {}).get('task_id') or self.job_id)
        if result is None:
            return None
        
        assert isinstance(result, bytes)
        return result

        # assert isinstance(result, bytes)
        # data = pickle.loads(result)
        # return data
