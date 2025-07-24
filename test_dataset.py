import ray
import click
import torch
import numpy as np
from typing import Dict

@click.command()
@click.option('-a', '--address',
              help='Ray cluster address'
                   '(use "192.168.0.7:6379" for LAN cluser, skip to run locally)')
@click.option('-s', '--shape', 'shape_str', default='1000,1000', show_default=True,
              help='Random array shape (one or more comma-separated dimensions)',)
@click.option('-b', '--batch', 'batch_size', type=int, default=0, show_default=True,
              help='Batch size (0 for whole batch processing)',)
@click.option('-n', '--concurrency', type=int, default=0, show_default=True,
              help='Concurrency RAY parameter (0 for default)',)
@click.option('-c', '--cpu', 'num_cpus', type=int, default=0, show_default=True,
              help='Number of CPUs reserved for each worker',)
@click.option('-g', '--gpu', 'num_gpus', type=int, default=0, show_default=True,
              help='Number of GPUs reserved for each worker',)
def main(
    address: str,
    shape_str: str,
    batch_size: int,
    concurrency: int,
    num_cpus: int,
    num_gpus: int,
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
        print(f'Executing as job {ray.get_runtime_context().get_job_id()} at {ray._private.services.get_node_ip_address()}') # type:ignore
        if num_gpus > 0:
            print(f'Computing batch with GPU: array{data.shape} * {x}')
            return {'data': (torch.from_numpy(data).cuda() * x).cpu().numpy()}
        else:
            print(f'Computing batch with CPU: array{data.shape} * {x}')
            return {'data': data * x}

    shape = tuple([int(x.strip()) for x in shape_str.split(',')])
    print(f'Source shape is {shape}')

    data = ray.data.from_numpy(np.random.uniform(0, 1, shape))
    print(f'Source sample: {next(iter(data.iter_batches(batch_size=10)))["data"][0][:10]}')

    result = data.map_batches(
        compute,
        batch_format='numpy',
        batch_size=batch_size if batch_size > 0 else 'default',
        concurrency=concurrency if concurrency > 0 else None,
        num_cpus=float(num_cpus) if num_cpus > 0 else None,
        num_gpus=float(num_gpus) if num_gpus > 0 else None,
        zero_copy_batch=True,
    )
    print(f'Result sample: {next(iter(result.iter_batches(batch_size=10)))["data"][0][:10]}')

if __name__ == '__main__':
    main()
