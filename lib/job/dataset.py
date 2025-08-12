import ray
import logging
import numpy as np
from lib.job.job import Job, Task, job
from typing import Tuple, Dict, Any

from lib.options import Options

_logger = logging.getLogger(__name__)

def dataset_setup(
        job: Job, 
        options: Options, 
        num_batches: int = 1, 
        shape: Tuple[int,int] = (1000,1000),
        **kwargs) -> Dict[str, Any]:
    
    if kwargs:
        _logger.warning(f'Extra arguments for compute() will be ignored {kwargs}')

    data = np.random.uniform(0, 1, shape) if num_batches <= 1 \
        else np.array([np.random.uniform(0, 1, shape) for _ in range(num_batches)])
    
    return {
        'data': ray.data.from_numpy(data),
        'has_batches': num_batches > 1,
    }

def _compute(batch: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
    x = np.random.rand()
    data = batch['data']
    result = data * x
    return { 'data': result }

@job(
    job_type='dataset', 
    supports_background=True,
    supports_batches=False,
    requirements={ 'num_cpus': 1 },
    setup_func=dataset_setup,
) # type:ignore
def dataset_compute(job: Job, 
                options: Options, 
                data: ray.data.Dataset, 
                batch_size: int = 0,
                concurrency: int = 1,
                **kwargs) -> Task:
    rows = data.map_batches(
            _compute,
            batch_format='numpy',
            batch_size=batch_size if batch_size > 0 else 'default',
            concurrency=concurrency if concurrency else None,
            num_cpus=job.requirements.get('num_cpus', 1),
            num_gpus=job.requirements.get('num_gpus', 0),
            zero_copy_batch=True,
            scheduling_strategy='SPREAD',
        ).take_all()
    
    return Task.from_output(
        parent=job,
        options=options,
        output=rows)

# class DatasetJob(Job):
#     """ Dataset processing job """

#     default_job_type: ClassVar[str] = 'dataset'
#     requirements: Dict[str, Any] = { 'num_cpus': 1  }
#     supports_background: bool = True

#     job_kwargs: Dict[str, Any] = Field(default_factory=dict, nullable=False, sa_type=JSON)
#     """ Job run arguments """

#     requirements: Dict[str, Any] = Field(default_factory=dict, nullable=False, sa_type=JSON)
#     """ Task requirements """


#     def setup(self, num_batches: int, shape: Tuple[int,...]) -> ray.data.Dataset:
#         np_data = np.random.uniform(0, 1, shape) if num_batches <= 1 \
#             else [np.random.uniform(0, 1, shape) for _ in range(num_batches)]
#         return ray.data.from_numpy(np_data)

#     @staticmethod
#     def _compute(batch: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
#         x = np.random.rand()
#         data = batch['data']
#         result = data * x
#         return { 'data': result }

#     @override
#     def run(self, 
#             options: Options,
#             is_background: bool = False,
#             num_batches: int = 3,
#             batch_size: int = 0,
#             concurrency: int = 1,
#             shape: Tuple[int,...] = (10,10),
#             **job_kwargs,
#             ) -> List['Task']:
#         """ Start a job and wait for result """
#         data = self.setup(num_batches, shape)
#         rows = data.map_batches(
#                 self._compute,
#                 batch_format='numpy',
#                 batch_size=batch_size if batch_size > 0 else 'default',
#                 concurrency=concurrency if concurrency else None,
#                 num_cpus=self.requirements.get('num_cpus'),
#                 num_gpus=self.requirements.get('num_gpus'),
#                 zero_copy_batch=True,
#                 scheduling_strategy='SPREAD',
#             ).take_all()
        
#         return [Task.from_output(
#             parent=self,
#             options=options,
#             output=rows,
#         )]

    # def stream(self, *, data: ray.data.Dataset) -> Generator[JobInstance, None, None]:
    #     ds = data.map_batches(
    #             self._compute,
    #             batch_format='numpy',
    #             batch_size=self.batch_size if self.batch_size > 0 else 'default',
    #             concurrency=self.requirements.get('concurrency'),
    #             num_cpus=self.requirements.get('num_cpus'),
    #             num_gpus=self.requirements.get('num_gpus'),
    #             zero_copy_batch=True,
    #             scheduling_strategy='SPREAD',
    #         )
    #     for batch in ds.iter_batches():
    #         yield JobInstance.from_output(
    #             parent=self,
    #             job_kwargs={
    #                 'num_batches': self.num_batches,
    #                 'batch_size': self.batch_size,
    #                 'concurrency': self.concurrency,
    #                 'shape': self.shape,
    #             },
    #             output=batch,
    #         )
