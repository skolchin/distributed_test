import os
import torch
import torch.nn as nn
import torch.nn.functional as F
import torch.optim as optim
from torch.utils.data import DataLoader
from torchvision import datasets, transforms
from typing import Dict, Any, Tuple

class ConvNet(nn.Module):
    """ Simple CNN """
    def __init__(self):
        super(ConvNet, self).__init__()
        self.conv1 = nn.Conv2d(1, 32, 3, 1)
        self.conv2 = nn.Conv2d(32, 64, 3, 1)
        self.dropout1 = nn.Dropout(0.25)
        self.dropout2 = nn.Dropout(0.5)
        self.fc1 = nn.Linear(9216, 128)
        self.fc2 = nn.Linear(128, 10)

    def forward(self, x):
        x = self.conv1(x)
        x = F.relu(x)
        x = self.conv2(x)
        x = F.relu(x)
        x = F.max_pool2d(x, 2)
        x = self.dropout1(x)
        x = torch.flatten(x, 1)
        x = self.fc1(x)
        x = F.relu(x)
        x = self.dropout2(x)
        x = self.fc2(x)
        output = F.log_softmax(x, dim=1)
        return output

def get_device(use_accel: bool) -> torch.device:
    """ Get Torch device """
    device: torch.device | None = None
    if use_accel:
        device = torch.accelerator.current_accelerator()
    if not device:
        device = torch.device("cpu")
    return device

def get_data_loaders(
        train_batch_size: int, 
        test_batch_size: int,
        use_accel: bool = True,
        root_dir: str = '~',
    ) -> Tuple[DataLoader, DataLoader]:
    """ Setup MNIST data loaders """

    train_kwargs: Dict[str, Any] = {'batch_size': train_batch_size}
    test_kwargs: Dict[str, Any]  = {'batch_size': test_batch_size}
    if use_accel:
        accel_kwargs = {
            'num_workers': 1,
            'persistent_workers': True,
            'pin_memory': True,
            'shuffle': True
        }
        train_kwargs.update(accel_kwargs)
        test_kwargs.update(accel_kwargs)

    transform = transforms.Compose([
        transforms.ToTensor(),
        transforms.Normalize((0.1307,), (0.3081,))
    ])

    data_path = os.path.join(os.path.expanduser(root_dir), '.cache', 'torch')
    os.makedirs(data_path, exist_ok=True)

    dataset1 = datasets.MNIST(data_path, train=True, download=True,
                       transform=transform)
    dataset2 = datasets.MNIST(data_path, train=False,
                       transform=transform)
    train_loader = DataLoader(dataset1, **train_kwargs)
    test_loader = DataLoader(dataset2, **test_kwargs)

    return train_loader, test_loader

def train_mnist(
        model: nn.Module, 
        device: torch.device, 
        train_loader: DataLoader, 
        optimizer: optim.Optimizer, 
        epoch: int, 
        log_interval: int = 10, 
        dry_run: bool = False):
    """ Train a model """
    
    model.train()
    for batch_idx, (data, target) in enumerate(train_loader):
        data, target = data.to(device), target.to(device)
        optimizer.zero_grad()
        output = model(data)
        loss = F.nll_loss(output, target)
        loss.backward()
        optimizer.step()
        if log_interval > 0 and batch_idx % log_interval == 0:
            print(f'Train epoch {epoch} loss: {loss.item():.6f}')
            if dry_run:
                break

def test_mnist(
        model: nn.Module, 
        device: torch.device, 
        test_loader: DataLoader,
    ) -> Dict[str, float]:
    """ Test a model """

    model.eval()
    test_loss = 0
    correct = 0
    with torch.no_grad():
        for data, target in test_loader:
            data, target = data.to(device), target.to(device)
            output = model(data)
            test_loss += F.nll_loss(output, target, reduction='sum').item()  # sum up batch loss
            pred = output.argmax(dim=1, keepdim=True)  # get the index of the max log-probability
            correct += pred.eq(target.view_as(pred)).sum().item()

    dataset_length: int = len(test_loader.dataset) #type:ignore
    test_loss /= dataset_length
    acc = 100. * correct / dataset_length

    return { 'loss': test_loss, 'accuracy': acc }
