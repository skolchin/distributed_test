import click
import torch
import torch.optim as optim
from torch.optim.lr_scheduler import StepLR

from mnist import ConvNet, get_device, get_data_loaders, train_mnist, test_mnist

@click.command()
@click.option('-n', '--epochs', type=int, default=14, show_default=True,
                help='number of epochs to train')
@click.option('--batch-size', type=int, default=64, show_default=True,
                help='input batch size for training')
@click.option('--test-batch-size', type=int, default=1000, show_default=True,
                help='input batch size for testing')
@click.option('--lr', type=float, default=0.063, show_default=True,
                help='learning rate')
@click.option('--gamma', type=float, default=0.7, show_default=True,
                help='Learning rate scheduler gamma')
@click.option('--no-accel', is_flag=True,
                help='disables accelerator')
@click.option('--dry-run', is_flag=True,
                help='quickly check with a single pass')
@click.option('--log-interval', type=int, default=10, show_default=True,
                help='how many batches to wait before logging training status')
@click.option('--save-model', is_flag=True, 
                help='Save the model at the end')
def main(
    epochs: int,
    batch_size: int,
    test_batch_size: int,
    lr: float,
    gamma: float,
    no_accel: bool,
    dry_run: bool,
    log_interval: int,
    save_model: bool,
):
    """ MNIST training with PyTorch """

    use_accel = not no_accel and torch.accelerator.is_available()
    device = get_device(use_accel)
    train_loader, test_loader = get_data_loaders(batch_size, test_batch_size, use_accel)
    model = ConvNet().to(device)
    optimizer = optim.Adadelta(model.parameters(), lr=lr)

    scheduler = StepLR(optimizer, step_size=1, gamma=gamma)
    for epoch in range(1, epochs + 1):
        train_mnist(model, device, train_loader, optimizer, epoch, log_interval, dry_run)
        metrics = test_mnist(model, device, test_loader)
        print(f'Test loss: {metrics["loss"]:.4f}, accuracy: {metrics["accuracy"]:.0f}%')
        
        scheduler.step()

    if save_model:
        torch.save(model.state_dict(), "mnist_cnn.pt")


if __name__ == '__main__':
    main()