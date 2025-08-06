import ray
import inspect
import logging
import importlib
from uuid import uuid4
from pathlib import Path
from abc import abstractmethod
from pydantic import BaseModel, Field, model_serializer
from typing import Any, Dict, List, TypedDict, Type, ClassVar, Generator

from lib.options import BackendOptions

_logger = logging.getLogger(__name__)

class JobType(TypedDict):
    """ Structured job type info """
    file_name: str
    """ Name of file where job type is defined """

    job_type: str
    """ Job type name"""

    job_cls: Type['Job']
    """ Job class"""

class Job(BaseModel):
    """ Ray job """

    job_type: ClassVar[str] = ''
    """ Job type """

    job_id: str = Field(default_factory=lambda: str(uuid4()))
    """ Internal job id """

    supports_background: bool = False
    """ Could be started in background """

    requirements: Dict[str, Any] = Field(default_factory=dict)
    """ Job resource requirements """

    _options: BackendOptions | None = None
    """ Service options """

    def setup(self, options: BackendOptions | None = None) -> Dict[str, Any] | None:
        """ Sets a job up """
        self._options = options

    def start(self, *args, **kwargs) -> ray.ObjectRef | List[ray.ObjectRef] | None:
        """ Start job with no wait for result """
        return None

    def run(self, *args, **kwargs) -> Any:
        """ Start job and wait for result """
        futures = self.start(*args, **kwargs)
        if futures is None:
            return None
        result = ray.get(futures)
        return result

    def stream(self, *args, **kwargs) -> Generator[Any | List[Any], None, None]:
        """ Start job in streaming mode """
        futures = self.start(*args, **kwargs)
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
                                job_type=mod_cls.job_type or mod_cls_name, 
                                job_cls=mod_cls))
                        globals()[mod_name] = mod

            except (ModuleNotFoundError, ImportError) as ex:
                print(ex)

        return result

    @model_serializer()
    def serialize_model(self):
        return {
            'job_type': self.job_type,
            'job_id': self.job_id,
            'supports_background': self.supports_background,
            'requirements': self.requirements,
        }
