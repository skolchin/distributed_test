import ray
import sys
import pickle
import numpy as np
from uuid import uuid4
from pathlib import Path
from datetime import datetime
from sqlalchemy import Column, JSON
from sqlmodel import SQLModel, Field
from ray.util import get_node_ip_address
from ray._private.worker import WORKER_MODE
from pydantic import PrivateAttr
from typing import Any, Dict

from lib.job.types import JobRuntimeInfo
from lib.job.job import Job

class JobInstance(SQLModel, table=True):
    """ Job instance """
    result_id: int | None = Field(default=None, primary_key=True)
    job_id: str = Field(index=True)
    job_type: str = Field(index=True)
    started: datetime = Field(default_factory=datetime.now)
    finished: datetime | None = Field(default=None)
    runtime_info: JobRuntimeInfo | None = Field(default=None, sa_column=Column(JSON))
    is_background: bool = Field(default=False)
    kwargs: Dict[str, Any] | None = Field(default=None, sa_column=Column(JSON))
    output: Any | None = Field(default=None, repr=False, sa_column=Column(JSON))
    _raw_output: Any | None = PrivateAttr(default=None)

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
                 kwargs: Dict[str, Any] | None = None,
                 output: Any | None = None) -> 'JobInstance':
        rt = cls.job_runtime_info()
        return cls(
            job_id=parent.job_id,
            job_type=parent.job_type,
            runtime_info=rt,
            kwargs=kwargs,
            _raw_output=output,
            output=cls._serialize_output(output, node_ip=rt.get('node_ip_address')),
        )

    @classmethod
    def from_output(cls, 
                    parent: Job, 
                    kwargs: Dict[str, Any],
                    output: Any) -> 'JobInstance':
        rt = cls.job_runtime_info()
        return cls(
            job_id=parent.job_id,
            job_type=parent.job_type,
            runtime_info=rt,
            kwargs=kwargs,
            _raw_output=output,
            output=cls._serialize_output(output, node_ip=rt.get('node_ip_address')),
        )

    @classmethod
    def _serialize_output(cls, data: Any, node_ip: str | None = None) -> Any:

        def get_size(x) -> int:
            match x:
                case np.ndarray():
                    return x.nbytes

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
