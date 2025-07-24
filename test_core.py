import ray
import click
import torch
import numpy as np
from colorama import Fore, Style

@click.command()
@click.option('-a', '--address',
              help='Ray cluster address'
                   '(use "192.168.0.7:6379" for LAN cluser, skip to run locally)')
@click.option('-s', '--shape', 'shape_str', default='2,1000,1000', show_default=True,
              help='Random array shape as one or more comma-separated dimensions.'
                   'If number of dimensions is >2, highest one will be used to split tasks')
@click.option('-f', '--force-affinity', is_flag=True,
              help='Enforce node affinity '
                    '(requires that at least one node will be published with `custom-resource` resource)',)
@click.option('-c', '--cpu', 'num_cpus', type=int, default=0, show_default=True,
              help='Number of CPUs reserved for each worker (0 for default)')
@click.option('-g', '--gpu', 'num_gpus', type=int, default=0, show_default=True,
              help='Number of GPUs reserved for each worker (0 for default)')
def main(
    address: str,
    shape_str: str,
    force_affinity: bool,
    num_cpus: int,
    num_gpus: int,
):
    """ Ray testing script (core) """

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

    res = {"custom-resource": 1.0} if force_affinity else {}
    @ray.remote(
        resources=res,
        num_cpus=num_cpus if num_cpus > 0 else None, #type:ignore
        num_gpus=num_gpus if num_gpus > 0 else None, #type:ignore
        scheduling_strategy='SPREAD',
    )
    def compute(batch: np.ndarray) -> np.ndarray:
        print(f'Executing as job {ray.get_runtime_context().get_job_id()} '
              f'at {Fore.GREEN}{ray._private.services.get_node_ip_address()}{Style.RESET_ALL}') # type:ignore
        
        x = np.random.rand()
        if num_gpus > 0:
            print(f'Computing batch with GPU: array{batch.shape} * {x}')
            result = (torch.from_numpy(batch).cuda() * x).cpu().detach().numpy()
        else:
            print(f'Computing batch with CPU: array{batch.shape} * {x}')
            result = batch * x
        return result

    shape = tuple([int(x.strip()) for x in shape_str.split(',')])
    if len(shape) <= 2:
        print(f'Source shape is {shape}')
        data = np.random.uniform(0, 1, shape)
        print(f'Source sample: {data[0][:10]}')

        futures = compute.remote(data)
        result = ray.get(futures)

        print(f'Result shape is {result.shape}')
        print(f'Result sample: {result[0][:10]}')
    else:
        num_workers, shape = shape[0], shape[1:]

        print(f'Source shape is {num_workers}x{shape}')
        data = [np.random.uniform(0, 1, shape) for _ in range(num_workers)]
        print(f'Source sample: {data[0][0][:10]}')

        futures = [compute.remote(d) for d in data]
        result = ray.get(futures)

        print(f'Result shape is {result[0].shape}')
        print(f'Result sample: {result[0][0][:10]}')

if __name__ == '__main__':
    main()
