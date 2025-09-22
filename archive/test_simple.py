import ray
import numpy as np

ray.init()

@ray.remote()
def compute():
    print('Hello, world!')

futures = compute.remote()
result = ray.get(futures)

