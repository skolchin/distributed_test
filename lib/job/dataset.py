import ray
import numpy as np
from lib.job.job import Job, Task
from typing import ClassVar, Tuple, override, Dict, Any, List, Generator

from lib.options import Options

class DatasetJob(Job):
    """ Dataset processing job """

    default_job_type: ClassVar[str] = 'dataset'
    requirements: Dict[str, Any] = { 'num_cpus': 1  }
    supports_background: bool = True

    def setup(self, num_batches: int, shape: Tuple[int,...]) -> ray.data.Dataset:
        np_data = np.random.uniform(0, 1, shape) if num_batches <= 1 \
            else [np.random.uniform(0, 1, shape) for _ in range(num_batches)]
        return ray.data.from_numpy(np_data)

    @staticmethod
    def _compute(batch: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
        x = np.random.rand()
        data = batch['data']
        result = data * x
        return { 'data': result }

    @override
    def run(self, 
            options: Options,
            is_background: bool = False,
            num_batches: int = 3,
            batch_size: int = 0,
            concurrency: int = 1,
            shape: Tuple[int,...] = (10,10),
            **job_kwargs,
            ) -> List['Task']:
        """ Start a job and wait for result """
        data = self.setup(num_batches, shape)
        rows = data.map_batches(
                self._compute,
                batch_format='numpy',
                batch_size=batch_size if batch_size > 0 else 'default',
                concurrency=concurrency if concurrency else None,
                num_cpus=self.requirements.get('num_cpus'),
                num_gpus=self.requirements.get('num_gpus'),
                zero_copy_batch=True,
                scheduling_strategy='SPREAD',
            ).take_all()
        
        return [Task.from_output(
            parent=self,
            options=options,
            output=rows,
        )]

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
