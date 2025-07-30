import ray
import logging
from fastapi import FastAPI
from contextlib import asynccontextmanager
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field, ConfigDict
from typing import AsyncIterator, Awaitable, Dict, Any, Callable, List

from lib.options import BackendOptions
from lib.conn import RayConnection
from lib.job.base import Job, JobTypeInfo

_logger = logging.getLogger(__name__)

class BackendState(BaseModel):
    """ Backend state """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    options: BackendOptions
    """ Application options """

    templates: Jinja2Templates = Field(exclude=True, repr=False)
    """ Templates environment """

    job_types: Dict[str, JobTypeInfo] = Field(default_factory=dict)
    """ Available job types """

    @property
    def connection(self) -> RayConnection:
        return RayConnection(options=self.options)

    @classmethod
    def from_defaults(cls) -> 'BackendState':
        """ Instantiate the state object """

        options = BackendOptions(_env_file='.env')   # type:ignore

        job_types = {}
        for job_info in Job.job_types():
            job_types[job_info['job_type']] = job_info

        return cls(
            options=options,
            templates=Jinja2Templates(directory="www/templates"),
            job_types=job_types,
        )

@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[Dict]:
    """ Application lifespan function """
    state = BackendState.from_defaults()
    app.state.backend_state = state
    yield {'backend_state': state}
