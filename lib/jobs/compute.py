import logging
import numpy as np
from lib.options import Options
from lib.job import remote_job, Job, Task

from typing import Tuple, Dict, Any

_logger = logging.getLogger(__name__)

def compute_setup(
        job: Job, 
        options: Options, 
        num_batches: int = 1, 
        shape: Tuple[int,int] = (1000,1000), 
        **kwargs) -> Dict[str, Any]:
    
    if kwargs:
        _logger.warning(f'Extra arguments will be ignored: {kwargs}')

    data = np.random.uniform(0, 1, shape) if num_batches <= 1 \
        else np.array([np.random.uniform(0, 1, shape) for _ in range(num_batches)])
    
    return {
        'data': data,
        'has_batches': num_batches > 1,
    }

@remote_job(
    job_type='compute', 
    supports_background=True,
    supports_batches=True,
    requirements={ 'num_cpus': 1 },
    setup_func=compute_setup,
) # type:ignore
def cpu_compute(job: Job, options: Options, data: np.ndarray, **kwargs) -> Task:
    x = np.random.rand()
    y = data * x
    raise ValueError('qq')

    return Task.from_output(
        parent=job,
        options=options,
        output=y)

@remote_job(
    job_type='gpu-compute', 
    supports_background=True,
    supports_batches=True,
    requirements={ 'num_gpus': 1 },
    setup_func=compute_setup,
) # type:ignore
def gpu_compute(job: Job, options: Options, data: np.ndarray, **kwargs) -> Task:
    import torch
    x = np.random.rand()
    x = torch.rand(1).cuda()
    y = torch.from_numpy(data).cuda()
    z = (x * y).cpu().detach().numpy()
    return Task.from_output(
        parent=job,
        options=options,
        output=z
    )


# class ComputeJob(Job):
#     """ Simple CPU-bound compute job """

#     cls_job_type: ClassVar[str] = 'compute'
#     cls_requirements: ClassVar[Dict[str, Any]] = { 'num_cpus': 1 }

#     num_batches: int = 3
#     shape: Tuple[int,...] = (10,10)

#     def setup(self) -> Dict[str, Any] | None:
#         data = np.random.uniform(0, 1, self.shape) if self.num_batches <= 1 \
#             else [np.random.uniform(0, 1, self.shape) for _ in range(self.num_batches)]
#         return { 'data': data }

#     @override
#     def start(self, input: Dict[str, Any] | None = None) -> ray.ObjectRef  | List[ray.ObjectRef]:
#         assert (input and 'data' in input)
#         data = input['data']

#         remote_kwargs = self.requirements.copy() | {
#             'scheduling_strategy': 'SPREAD',
#         }
#         @ray.remote(**remote_kwargs)
#         def cpu_compute(batch: np.ndarray) -> Task:
#             x = np.random.rand()
#             return Task.from_output(
#                 parent=self,
#                 output=batch*x)
        
#         return cpu_compute.remote(data) if self.num_batches <= 1 else \
#             [cpu_compute.remote(d) for d in data]

# class GPUComputeJob(ComputeJob):
#     """ Simple GPU-bound compute job """
#     cls_job_type: ClassVar[str] = 'gpu_compute'
#     cls_requirements: ClassVar[Dict[str, Any]] = { 'num_cpus': 0, 'num_gpus': 1 }

#     @override
#     def start(self, input: Dict[str, Any] | None = None) -> ray.ObjectRef  | List[ray.ObjectRef]:
#         import torch

#         assert (input and 'data' in input)
#         data = input['data']

#         remote_kwargs = self.requirements.copy() | {
#             'scheduling_strategy': 'SPREAD',
#         }
#         @ray.remote(**remote_kwargs)
#         def gpu_compute(batch: np.ndarray) -> Task:
#             x = torch.rand(1).cuda()
#             y = torch.from_numpy(batch).cuda()
#             z = (x * y).cpu().detach().numpy()
#             return Task.from_output(
#                 parent=self,
#                 output=z
#             )

#         return gpu_compute.remote(data) if self.num_batches <= 1 else \
#             [gpu_compute.remote(d) for d in data]

