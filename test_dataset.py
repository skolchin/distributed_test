import ray
import numpy as np
from typing import Dict

ray.init()

def compute(batch: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
    x = np.random.rand()
    data = batch['data']
    print(f'Computing: array{data.shape} * {x}')
    return {'data': data * x}

data = ray.data.from_numpy(np.random.uniform(0, 1, size=(10, 1000)))

print(f'Source sample: {next(iter(data.iter_batches(batch_size=10)))["data"][0][:10]}')

result = data.map_batches(compute)

print(f'Result sample: {next(iter(result.iter_batches(batch_size=10)))["data"][0][:10]}')
