import ray
import numpy as np
from lib.job.job import Job
from lib.job.types import JobRuntimeInfo
from lib.job.job_instance import JobInstance
from typing import ClassVar, Tuple, override, Dict, Any, List

class ComputeJob(Job):
    """ Simple CPU-bound compute job """

    job_type: ClassVar[str] = 'compute'
    requirements: Dict[str, Any] = { 'num_cpus': 1 }
    supports_background: bool = True

    num_batches: int = 3
    shape: Tuple[int,...] = (1000,1000)

    def setup(self):
        data = np.random.uniform(0, 1, self.shape) if self.num_batches <= 1 \
            else [np.random.uniform(0, 1, self.shape) for _ in range(self.num_batches)]
        return { 'data': data }

    @override
    def start(self, *, data: np.ndarray) -> ray.ObjectRef  | List[ray.ObjectRef]:
        remote_kwargs = self.requirements.copy() | {
            'scheduling_strategy': 'SPREAD',
        }
        @ray.remote(**remote_kwargs)
        def cpu_compute(batch: np.ndarray) -> JobInstance:
            x = np.random.rand()
            return JobInstance.from_output(
                parent=self,
                kwargs={
                    'num_batches': self.num_batches,
                    'shape': self.shape,
                },
                output=batch*x)
        
        return cpu_compute.remote(data) if self.num_batches <= 1 else \
            [cpu_compute.remote(d) for d in data]

class GPUComputeJob(ComputeJob):
    """ Simple GPU-bound compute job """
    job_type: ClassVar[str] = 'gpu_compute'
    requirements: Dict[str, Any] = { 'num_cpus': 0, 'num_gpus': 1 }

    @override
    def start(self, *, data: np.ndarray) -> ray.ObjectRef  | List[ray.ObjectRef]:
        import torch

        remote_kwargs = self.requirements.copy() | {
            'scheduling_strategy': 'SPREAD',
        }
        @ray.remote(**remote_kwargs)
        def gpu_compute(batch: np.ndarray) -> JobInstance:
            x = torch.rand(1).cuda()
            y = torch.from_numpy(batch).cuda()
            z = (x * y).cpu().detach().numpy()
            return JobInstance.from_output(
                parent=self,
                kwargs={
                    'num_batches': self.num_batches,
                    'shape': self.shape,
                },
                output=z
            )

        return gpu_compute.remote(data) if self.num_batches <= 1 else \
            [gpu_compute.remote(d) for d in data]

