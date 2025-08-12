from pydantic import BaseModel, Field, ConfigDict
from typing import List, Dict, Any, Sequence

from lib.job.job import Job, JobBase, Task

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

class JobResponse(JobBase):
    """ Single job response """

    tasks: list['Task'] = Field(default_factory=list)
    """ List of job tasks """

    @classmethod
    def from_job(cls, job: Job, add_tasks: bool = True) -> 'JobResponse':
        o = cls.model_validate(job.model_dump())
        if add_tasks:
            o.tasks = [t for t in job.tasks]
        return o

class JobListResponse(BaseModel):
    """ Job list response """
    jobs: Sequence[JobResponse] = Field(default_factory=list)
    """ List of job """

    @classmethod
    def from_jobs(cls, jobs: Sequence[Job]) -> 'JobListResponse':
        my_jobs = []
        for j in jobs:
            o = JobResponse.model_validate(j.model_dump())
            o.tasks = [t for t in j.tasks]
            my_jobs.append(o)
            
        return cls(jobs=my_jobs)

class JobResultResponse(BaseModel):
    """ Single job result response """
    job: Job
    """ Job info """

    output: bytes
    """ job result """

    max_chunk: int = 0
    """ number of chunks """
