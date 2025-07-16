import ray
import click
import numpy as np
from typing import Dict

@click.command()
@click.option('-a', '--address',
              help='Ray cluster address'
                   '(use "192.168.0.7:6379" for LAN cluser, skip to run locally)')
@click.option('-s', '--shape', 'shape_str', default='1000,1000', show_default=True,
              help='Random array shape (one or more comma-separated dimensions)',)
def main(
    address: str,
    shape_str: str,
):
    """ Ray testing script (datasets) """

    if not address:
        print('Using local Ray instance')
    else:
        if not address.partition(':')[2]:
            address = address + ':6379'
        print(f'Will use Ray cluster at {address}')

    ray.init(
        address=address,
        log_to_driver=True,
        runtime_env={
            "env_vars": {"RAY_DEBUG": "1"}, 
        }
    )

    def compute(batch: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
        x = np.random.rand()
        data = batch['data']
        print(f'Computing: array{data.shape} * {x}')
        return {'data': data * x}

    shape = tuple([int(x.strip()) for x in shape_str.split(',')])
    print(f'Source shape is {shape}')

    data = ray.data.from_numpy(np.random.uniform(0, 1, shape))
    print(f'Source sample: {next(iter(data.iter_batches(batch_size=10)))["data"][0][:10]}')

    result = data.map_batches(compute)
    print(f'Result sample: {next(iter(result.iter_batches(batch_size=10)))["data"][0][:10]}')

if __name__ == '__main__':
    main()
