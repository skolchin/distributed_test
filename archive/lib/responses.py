from datetime import datetime
from pydantic import BaseModel, Field, ConfigDict
from typing import List, Dict, Any, Sequence

from lib.job import Job, JobBase, Task

class ClusterStatusResponse(BaseModel):
    """ Cluster status response """

    cluster_address: str
    """ Cluster IP address or domain name """

    nodes: List[Dict[str, Any]] = Field(default_factory=list)
    """ Cluster nodes """

    resources: Dict[str, Any] = Field(default_factory=dict)
    """ Cluster resources """

class ClusterOperationResponse(BaseModel):
    """ Cluster operation response """

    cluster_address: str
    """ Cluster IP address or domain name """

class NodeStatusResponse(BaseModel):
    """ Node status response """

    node_address: str
    """ Node IP address or domain name """

class NodeOperationResponse(BaseModel):
    """ Node operation response """

    node_address: str
    """ Node IP address or domain name """

class JobTypesResponse(BaseModel):
    """ Job types response """
    job_types: List[str] = Field(default_factory=list)
    """ Job type """

class JobSubmitRequest(BaseModel):
    """ Job submit request """
    job_type: str
    """ Job type """

    as_background: bool = False
    """ Attempt to run in background """

    stream: bool = False
    """ Attempt to run in streaming mode """

    model_config = ConfigDict(extra='allow')

def _safe_min(it):
    return min(it) if it else None

def _safe_max(it):
    return max(it) if it else None

class JobResponse(JobBase):
    """ Single job response """

    started: datetime | None = Field(None)
    finished: datetime | None = Field(None)

    tasks: list['Task'] = Field(default_factory=list)
    """ List of job tasks """

    @classmethod
    def from_job(cls, job: Job, add_tasks: bool = True) -> 'JobResponse':
        resp = cls.model_validate(job.model_dump())
        if add_tasks:
            resp.tasks = [t for t in job.tasks]
            resp.started = _safe_min([t.started for t in resp.tasks if t.started])
            resp.finished = _safe_max([t.finished for t in resp.tasks if t.finished])
        return resp

class JobListResponse(BaseModel):
    """ Job list response """
    jobs: Sequence[JobResponse] = Field(default_factory=list)
    """ List of job """

    @classmethod
    def from_jobs(cls, jobs: Sequence[Job]) -> 'JobListResponse':
        resps = [JobResponse.from_job(j) for j in jobs]
        return cls(jobs=resps)
