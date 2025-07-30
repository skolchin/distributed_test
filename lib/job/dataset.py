import ray
import numpy as np
from lib.job.job import Job
from lib.job.types import JobRuntimeInfo
from lib.job.job_instance import JobInstance
from typing import ClassVar, Tuple, override, Dict, Any, List, Generator, cast

class DatasetJob(Job):
    """ Dataset processing job """

    job_type: ClassVar[str] = 'dataset'
    requirements: Dict[str, Any] = { 'num_cpus': 1  }
    supports_background: bool = True

    num_batches: int = 3
    batch_size: int = 0
    concurrency: int = 1
    shape: Tuple[int,...] = (1000,1000)

    def setup(self):
        np_data = np.random.uniform(0, 1, self.shape) if self.num_batches <= 1 \
            else [np.random.uniform(0, 1, self.shape) for _ in range(self.num_batches)]
        data = ray.data.from_numpy(np_data)
        return { 'data': data }

    @staticmethod
    def _compute(batch: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
        x = np.random.rand()
        data = batch['data']
        result = data * x
        return { 'data': result }

    @override
    def run(self, *, data: ray.data.Dataset) -> JobInstance:
        rows = data.map_batches(
                self._compute,
                batch_format='numpy',
                batch_size=self.batch_size if self.batch_size > 0 else 'default',
                concurrency=self.concurrency if self.concurrency else None,
                num_cpus=self.requirements.get('num_cpus'),
                num_gpus=self.requirements.get('num_gpus'),
                zero_copy_batch=True,
                scheduling_strategy='SPREAD',
            ).take_all()
        
        return JobInstance.from_output(
            parent=self,
            kwargs={
                'num_batches': self.num_batches,
                'batch_size': self.batch_size,
                'concurrency': self.concurrency,
                'shape': self.shape,
            },
            output=rows,
        )

    def stream(self, *, data: ray.data.Dataset) -> Generator[JobInstance, None, None]:
        ds = data.map_batches(
                self._compute,
                batch_format='numpy',
                batch_size=self.batch_size if self.batch_size > 0 else 'default',
                concurrency=self.requirements.get('concurrency'),
                num_cpus=self.requirements.get('num_cpus'),
                num_gpus=self.requirements.get('num_gpus'),
                zero_copy_batch=True,
                scheduling_strategy='SPREAD',
            )
        for batch in ds.iter_batches():
            yield JobInstance.from_output(
                parent=self,
                kwargs={
                    'num_batches': self.num_batches,
                    'batch_size': self.batch_size,
                    'concurrency': self.concurrency,
                    'shape': self.shape,
                },
                output=batch,
            )
