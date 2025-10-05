# Distributed computing

This is a project to explore very advanced distributed computing framework of [Ray](https://docs.ray.io/en/latest/index.html)

## Setup

A [HuggingFace](http://huggingface.co/) API key is required in order to download models from the hub.
Look at [this guide](https://huggingface.co/docs/hub/en/security-tokens) to find out how to generate one.

Generated API key has to be assigned to `HUGGINGFACE_API_KEY` environment variable or saved to
`.env` file at project's root directory.

## Starting cluster

Run:

```bash
docker compose --profile server up --build
```

This will launch Ray head node (initiating a cluster), wait for 30 sec and
then launch VLLM serve with a model specified in `MODEL` environment variable.
The model will be distributed across all nodes which joined the cluster within
than 30 seconds.

Default model is `Qwen/Qwen3-0.6B`. To start arbitrary model, set `MODEL` variable before starting up, like:

```bash
MODEL='Qwen/Qwen3-4B-Thinking-2507' docker compose --profile server up --build
```

## Starting a node

Run:

```bash
docker compose --profile client up -d --build
```

Note that node must start within 30 seconds after ray cluster initiated, or
it will not be counted when vllm is started. Dynamic re-allocation is to be done.
