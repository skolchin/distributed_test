from uuid import uuid4
from pydantic import BaseModel, Field, ConfigDict
from typing import List, Dict, Any, Sequence

from lib.job.job import Job
from lib.job.job_instance import JobInstance

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

class JobSubmitRequest(BaseModel):
    """ Job submit request """
    job_type: str
    """ Job type """

    as_background: bool = False
    """ Attempt to run in background """

    model_config = ConfigDict(extra='allow')

class JobInstanceResponse(BaseModel):
    """ List of jobs result response """
    jobs: Sequence[Job] = Field(default_factory=list)
    job_instances: Sequence[JobInstance] = Field(default_factory=list)

