# Distributed computing

This is a project to explore very advanced distributed computing framework of [Ray](https://docs.ray.io/en/latest/index.html)
along with not less sophisticated LLM distributed serving library [vLLM](https://docs.vllm.ai/en/stable/index.html).

## Setup

Primary OS for development is Unix, but Windows should be fine too as all work is done in Docker containers.

Requirements:

1. Modern GPU with at least 8 GB RAM. CPU / RAM is not that important.

2. Python 3.11+. New Python environment is recommended, create it like this:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

3. A [HuggingFace](http://huggingface.co/) API key is required in order to download models from the hub.
Look at [this guide](https://huggingface.co/docs/hub/en/security-tokens) to find out how to generate one.

4. Docker runtime or Docker Desktop.

## Starting cluster

On a head (master) machine, run (for unix):

```bash
./start_docker.sh --head
```

or (for Windows):

```powershell
.\start_docker.ps1 -NodeType head
```

This will create a Ray cluster and start the head node. Only one head node is needed for a cluster.

Note that this command will not return till the cluster is running.

## Starting a node

On a client computer, run (for unix):

```bash
./start_docker.sh --worker head_node_ip
```

or (for Windows):

```powershell
.\start_docker.ps1 -NodeType worker -HeadNodeAddress head_node_ip
```

This will launch a Ray node and join it with the cluster specified by head_node_ip.

Note that this command will not return till the node is running.

## Starting VLLM

On a HEAD node, run (for unix):

```bash
./start_vllm.sh model [huggingface_api_key] [--additional_args]
```

or (for Windows):

```powershell
.\start_vllm.ps1 -Model model [-HuggingfaceApiKey huggingface_api_key] [--additional_args ...]
```

This will download a model from HuggingFace and serve it with vllm.
The model will be distributed across all GPU nodes of a cluster.

The model is downloaded only once and saved to `~/.cache/huggingface` directory.
For example, to serve small Qwen3 model, run:

```bash
./start_vllm.sh Qwen/Qwen3-0.6B
```

## Testing VLLM

Run Python script `test_vllm.py` and chat with the model from command line.
Model which is currently deployed to the cluster will be recognized automatically.

## Monitoring

To launch Prometheus and Grafana, build and run docker containers. `GRAFANA_ADMIN_PASSWORD` env variable has to be set,
for example from the command line:

``` bash
GRAFANA_ADMIN_PASSWORD="secret" docker compose -f ./docker/docker-compose-metrics.yaml up -d --build
```


Prometheus is set up to collect ray and vllm metrics automatically. Its UI
is available at http://localhost:9090/.

Grafana is currently launched as is.
