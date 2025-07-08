import ray
import numpy as np

ray.init(address="ray://192.168.0.7:10001", log_to_driver=True)

# @ray.remote(resources={"my_resource": 1})
@ray.remote()
def compute(batch: np.ndarray) -> np.ndarray:
    x = np.random.rand()
    print(f'Executing as job {ray.get_runtime_context().get_job_id()} at {ray._private.services.get_node_ip_address()}') # type:ignore
    print(f'Computing: array{batch.shape} * {x}')
    return batch * x

data = np.random.uniform(0, 1, (1000, 1000))
print(f'Source sample ([0,:10] out of {data.shape}): {data[0][:10]}')

futures = compute.remote(data)
result = ray.get(futures)

print(f'Result sample ([0,:10] out of {result.shape}): {result[0][:10]}')
