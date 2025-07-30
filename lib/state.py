import ray
import logging
from fastapi import FastAPI
from sqlmodel import SQLModel, create_engine
from sqlalchemy.engine import Engine
from contextlib import asynccontextmanager
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field, ConfigDict

from lib.options import BackendOptions
from lib.conn import RayConnection
from lib.job.job import Job, JobType

from typing import AsyncIterator, Dict

_logger = logging.getLogger(__name__)

class BackendState(BaseModel):
    """ Backend state """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    options: BackendOptions
    """ Application options """

    templates: Jinja2Templates = Field(exclude=True, repr=False)
    """ Templates environment """

    job_types: Dict[str, JobType] = Field(default_factory=dict)
    """ Available job types """

    engine: Engine | None = Field(default=None, exclude=True, repr=False)

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

        sqlite_url = f"sqlite:///{options.sqlite_file_name}"
        connect_args = { "check_same_thread": False }
        engine = create_engine(sqlite_url, echo=False, connect_args=connect_args)
        _logger.info(f'Connected to database {sqlite_url}')

        SQLModel.metadata.create_all(engine)

        return cls(
            options=options,
            templates=Jinja2Templates(directory="www/templates"),
            job_types=job_types,
            engine=engine,
        )

@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[Dict]:
    """ Application lifespan function """
    state = BackendState.from_defaults()
    app.state.backend_state = state
    yield {'backend_state': state}
