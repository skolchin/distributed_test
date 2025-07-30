from typing import Any, Dict, List, TypedDict, Type, ClassVar

class JobRuntimeInfo(TypedDict):
    """ Job runtime information """
    ray_job_id: str
    node_id: str
    node_name: str
    node_ip_address: str
    worker_id: str | None
    task_id: str | None
    resources: Dict[str, Any] | None
