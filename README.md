# Distributed computing

This is a project to explore very advanced distributed computing framework of [Ray](https://docs.ray.io/en/latest/index.html)

## Starting cluster

Run:

```bash
docker compose --profile server up -d --build
```

## Starting a node

Run:

```bash
docker compose --profile client up -d --build
```


## Starting vLLM model

After starting the cluster, run:

```bash
docker compose --profile vllm up -d --build
```

This will load default model (`Qwen/Qwen3-0.6B`) and serve it with vLLM on the Ray cluster.
See `test_vllm.py` on how to use it.

To start arbitrary model, set `MODEL` variable before starting up, like:

```bash
MODEL='Qwen/Qwen3-4B-Thinking-2507' docker compose --profile vllm up -d --build
```
