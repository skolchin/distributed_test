import ray
import numpy as np

ray.init()

@ray.remote
def compute(batch: np.ndarray) -> np.ndarray:
    x = np.random.rand()
    print(f'Computing: array{batch.shape} * {x}')
    return batch * x

data = np.random.uniform(0, 1, (1000, 1000))
# data = ray.data.range_tensor(1000, shape=(2,2))

print(f'Source sample ([0,:10] out of {data.shape}): {data[0][:10]}')

futures = compute.remote(data)
result = ray.get(futures)

print(f'Result sample ([0,:10] out of {result.shape}): {result[0][:10]}')
