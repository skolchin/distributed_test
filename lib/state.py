import ray
import logging
from fastapi import FastAPI
from sqlmodel import SQLModel, create_engine
from sqlalchemy.engine import Engine
from contextlib import asynccontextmanager
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field, ConfigDict

from lib.options import Options
from lib.conn import RayConnection
from lib.job import Job, JobType
from lib.json_utils import sqla_json_serializer, sqla_json_deserializer

from typing import AsyncIterator, Dict

_logger = logging.getLogger(__name__)

class BackendState(BaseModel):
    """ Backend state """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    options: Options
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

        options = Options(_env_file='.env')   # type:ignore

        job_types = {}
        for job_info in Job.job_types():
            if job_info['job_cls']:
                job_type = job_info['job_cls'].default_job_type
            elif job_info['job_func']:
                job = job_info['job_func']()
                assert isinstance(job, Job)
                job_type = job.job_type
            else:
                raise ValueError(f'Neither class nor function')

            job_types[job_type] = job_info

        sqlite_url = f"sqlite:///{options.sqlite_file_name}"
        connect_args = { "check_same_thread": False }
        engine = create_engine(
            sqlite_url,
            echo=False,
            connect_args=connect_args,
            json_serializer=sqla_json_serializer,
            json_deserializer=sqla_json_deserializer,
        )
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
