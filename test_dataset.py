import ray
import click
import torch
import numpy as np
from time import sleep
from typing import Dict
from colorama import Fore, Style

@click.command()
@click.option('-a', '--address',
              help='Ray cluster address'
                   '(use "192.168.0.7:6379" for LAN cluser, skip to run locally)')
@click.option('-s', '--shape', 'shape_str', default='2,1000,1000', show_default=True,
              help='Random array shape as one or more comma-separated dimensions.'
                   'If number of dimensions is >2, highest one will be used to split by blocks')
@click.option('-b', '--batch', 'batch_size', type=int, default=0, show_default=True,
              help='Batch size (0 for whole batch processing)',)
@click.option('-n', '--concurrency', type=int, default=0, show_default=True,
              help='Concurrency parameter (0 for default)')
@click.option('-c', '--cpu', 'num_cpus', type=int, default=0, show_default=True,
              help='Number of CPUs reserved for each worker (0 for default)')
@click.option('-g', '--gpu', 'num_gpus', type=int, default=0, show_default=True,
              help='Number of GPUs reserved for each worker (0 for default)')
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
        print(f'Executing as job {ray.get_runtime_context().get_job_id()} '
              f'at {Fore.GREEN}{ray._private.services.get_node_ip_address()}{Style.RESET_ALL}') # type:ignore
        
        x = np.random.rand()
        data = batch['data']

        if num_gpus > 0:
            print(f'Computing batch with GPU: array{data.shape} * {x}')
            result = (torch.from_numpy(data).cuda() * x).cpu().numpy()
        else:
            print(f'Computing batch with CPU: array{data.shape} * {x}')
            result = data * x

        return { 'data': result }

    shape = tuple([int(x.strip()) for x in shape_str.split(',')])
    if len(shape) <= 2:
        np_data = np.random.uniform(0, 1, shape)
        print(f'Source shape is {shape}')
        print(f'Source sample: {np_data.flatten()[:10]}')
    else:
        num_blocks, shape = shape[0], shape[1:]
        print(f'Source shape is {num_blocks}x{shape}')
        np_data = [np.random.uniform(0, 1, shape) for _ in range(num_blocks)]
        print(f'Source sample: {np_data[0].flatten()[:10]}')

    if num_gpus and not batch_size:
        batch_size = shape[1] if len(shape) > 1 else 100

    data = ray.data.from_numpy(np_data)
    result = data.map_batches(
        compute,
        batch_format='numpy',
        batch_size=batch_size if batch_size > 0 else 'default',
        concurrency=concurrency if concurrency > 0 else None,
        num_cpus=float(num_cpus) if num_cpus > 0 else None,
        num_gpus=float(num_gpus) if num_gpus > 0 else None,
        zero_copy_batch=True,
        scheduling_strategy='SPREAD',
    ).take_all()

    print(f'Result shape is {len(result)}x{result[0]["data"].shape}')
    print(f'Result sample: {result[0]["data"][:10]}')

if __name__ == '__main__':
    main()
