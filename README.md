# Distributed computing

This is a project to explore very advance distributing computing framework of [Ray](https://docs.ray.io/en/latest/index.html)

## Requirements

Python 3.12.10

## Setup

1. For Windows: set `RAY_ENABLE_WINDOWS_OR_OSX_CLUSTER=1`

2. Make virtual environment:

Unix:

```bash
python3.10 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Windows:

```powershell
python3.10 -m venv .venv
.\.venv\scripts\activate.ps1
pip install -r requirements.txt
```

## Starting cluster

Use:

```bash
./start_cluster.sh <address>
```

where <address> is optional public cluster address.

## Starting node

Use:

```bash
./start_node.sh <address>
```

or

```powershell
.\start_node.ps1 <address>
```

where the <address> is a cluster public address.

Make sure that 44403 and 10001-19999 ports are opened on firewall.

