# patches: 
#
# .venv/lib/python3.12/site-packages/ray/train/_internal/session.py:950
#       return session.world_size
#           ->
#       return session.world_size or 0
#
# .venv/lib/python3.12/site-packages/ray/train/_internal/session.py:989
#       return session.world_rank
#           ->
#       return session.world_rank or 0
#
import os
import ray
import click
import torch
import tempfile
import torch.optim as optim
from ray import tune
from colorama import Fore, Style
from ray.tune.experimental.output import AirVerbosity
from ray.tune.stopper import ExperimentPlateauStopper
from ray.tune.schedulers import AsyncHyperBandScheduler
from ray.train.torch import prepare_model, prepare_data_loader

from mnist import ConvNet, get_device, get_data_loaders, train_mnist, test_mnist

def job_func(config: dict):

    print(f'Executing as job {ray.get_runtime_context().get_job_id()} '
            f'at {Fore.GREEN}{ray._private.services.get_node_ip_address()}{Style.RESET_ALL}') # type:ignore

    epochs = config['epochs']
    batch_size = config['batch_size']
    test_batch_size = config['test_batch_size']
    log_interval = config['log_interval']
    no_accel = config['no_accel']
    save_checkpoint = config['save_checkpoint']
    lr = config['lr']

    use_accel = not no_accel and torch.accelerator.is_available()
    device = get_device(use_accel)
    model = prepare_model(ConvNet().to(device), move_to_device=False, parallel_strategy='ddp')
    train_loader, test_loader = get_data_loaders(batch_size, test_batch_size, use_accel)
    train_loader = prepare_data_loader(train_loader, move_to_device=False)
    test_loader = prepare_data_loader(test_loader, move_to_device=False)
    optimizer = optim.Adadelta(model.parameters(), lr=lr)

    for epoch in range(epochs):
        train_mnist(model, device, train_loader, optimizer, epoch, log_interval)
        metrics = test_mnist(model, device, test_loader) | { 'epoch': epoch }

        # Report metrics (and possibly a checkpoint)
        if save_checkpoint:
            with tempfile.TemporaryDirectory() as tempdir:
                torch.save(model.state_dict(), os.path.join(tempdir, 'model.pt'))
                tune.report(metrics, checkpoint=tune.Checkpoint.from_directory(tempdir))
        else:
            tune.report(metrics)


@click.command()
@click.option('-a', '--address',
                help='Ray cluster address'
                     '(use `192.168.0.7:6379` for LAN cluser, skip to run locally)')
@click.option('-c', '--cpu', 'num_cpus', type=int, default=0, show_default=True,
                help='Number of CPUs reserved for each worker (0 for no particular allocation)')
@click.option('-g', '--gpu', 'num_gpus', type=int, default=0, show_default=True,
                help='Number of GPUs reserved for each worker (0 for no particular allocation)')
@click.option('-n', '--epochs', type=int, default=14, show_default=True,
                help='Number of epochs to train')
@click.option('-j', '--jobs', type=int, default=10, show_default=True,
                help='Number of tasks to start')
@click.option('--batch-size', type=int, default=64, show_default=True,
                help='Input batch size for training')
@click.option('--test-batch-size', type=int, default=1000, show_default=True,
                help='Input batch size for testing')
@click.option('--no-accel', is_flag=True, 
                help='Disables accelerator')
@click.option('--save-model', is_flag=True,
                help='Save checkpoints')
def main(
    address: str,
    num_cpus: int,
    num_gpus: int,
    epochs: int,
    batch_size: int,
    test_batch_size: int,
    no_accel: bool,
    save_model: bool,
    jobs: int,
):
    """ MNIST training with Ray """

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
            'env_vars': {
                'RAY_DEBUG': '1',
                'RAY_DEDUP_LOGS': '0',
            },
        }
    )

    if no_accel:
        num_cpus = num_cpus or 1
        num_gpus = 0
    elif not num_cpus and not num_gpus:
        if no_accel: 
            num_cpus = 1
        else:
            num_gpus = 1

    tuner = tune.Tuner(
        tune.with_resources(
            job_func,
            resources={
                'cpu': float(num_cpus), 
                'gpu': float(num_gpus)
            },
        ),
        tune_config=tune.TuneConfig(
            metric='accuracy',
            mode='max',
            num_samples=jobs,
            scheduler=AsyncHyperBandScheduler(
                time_attr='epoch',
            ),
        ),
        run_config=tune.RunConfig(
            name='mnist',
            verbose=AirVerbosity.VERBOSE,
            failure_config=tune.FailureConfig(
                max_failures=1,
            ),
            stop=ExperimentPlateauStopper(
                metric='accuracy',
                mode='max',
                patience=5,
            )
        ),
        param_space={
            'epochs': epochs,
            'batch_size': batch_size,
            'test_batch_size': test_batch_size,
            'no_accel': no_accel,
            'log_interval': -1,
            'save_checkpoint': save_model,
            'lr': tune.loguniform(1e-4, 1e-2),
        },
    )
    results = tuner.fit()
    best_result = results.get_best_result()
    assert best_result.metrics
    print(f'Best trial: loss {best_result.metrics["loss"]:.4f}, accuracy {best_result.metrics["accuracy"]:.2f}%')

if __name__ == '__main__':
    main()
