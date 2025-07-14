import ray
import click
import numpy as np
from urllib.parse import urlparse, urlunparse

@click.command()
@click.option('-a', '--address',
              help='Ray cluster address or URL'
                   '(use "ray://192.168.0.7:10001" for LAN cluser, skip to run locally)')
@click.option('-s', '--shape', 'shape_str', default='1000,1000', show_default=True,
              help='Random array shape (one or more comma-separated dimensions)',)
@click.option('-f', '--force-affinity', is_flag=True,
              help='Enforce node affinity '
                    '(requires that at least one node will be published with `custom-resource` resource)',)
def main(
    address: str,
    shape_str: str,
    force_affinity: bool,
):
    """ Ray testing script (core) """

    if not address:
        print('Using local Ray instance')
    else:
        ps = urlparse(address, scheme='ray', allow_fragments=False)
        if not ps.port:
            ps = ps._replace(port=1001)
        address = urlunparse(ps)
        print(f'Will use Ray cluster at {address}')

    ray.init(
        address=address,
        log_to_driver=True,
        runtime_env={
            "env_vars": {"RAY_DEBUG": "1"}, 
        }
    )

    res = {"custom-resource": 1.0} if force_affinity else {}
    @ray.remote(resources=res)
    def compute(batch: np.ndarray) -> np.ndarray:
        x = np.random.rand()
        print(f'Executing as job {ray.get_runtime_context().get_job_id()} at {ray._private.services.get_node_ip_address()}') # type:ignore
        print(f'Computing: array{batch.shape} * {x}')
        return batch * x

    shape = tuple([int(x.strip()) for x in shape_str.split(',')])
    print(f'Source shape is {shape}')

    data = np.random.uniform(0, 1, shape)
    print(f'Source sample ([0,:10] out of {data.shape}): {data[0][:10]}')

    futures = compute.remote(data)
    result = ray.get(futures)

    print(f'Result sample ([0,:10] out of {result.shape}): {result[0][:10]}')

if __name__ == '__main__':
    main()
