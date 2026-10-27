# WarmServe

A Go inference gateway exploring warm-worker reuse, bounded admission, model-affine scheduling and scale-to-zero for Python/PyTorch workers.

## What runs

The gateway starts real child processes on a cold request, reserves capacity during startup, reuses a matching idle worker, and evicts the least recently used idle worker when capacity is full. Busy workers are never evicted. A background reaper terminates workers after their idle TTL. Cancellation, failed startup and unhealthy upstream responses release capacity.

The Python worker runs a tiny randomly initialized embedding/GRU text model on CPU. It demonstrates inference lifecycle management, **not pretrained LLM quality, GPU throughput, distributed scheduling or container startup performance**. Request bodies are bounded, model names allowlisted and prompts excluded from structured logs. This is a local engineering project, not a production service: add authentication, authorization, tenant isolation and deployment hardening before exposing it.

## Run locally

Requires Go 1.24+ and Python 3.12+ with PyTorch (tested with torch 2.10.0).

```sh
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
WARMSERVE_PYTHON="$PWD/.venv/bin/python" go run .
```

```sh
curl http://127.0.0.1:8088/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{"model":"demo-a","messages":[{"role":"user","content":"hello"}],"max_tokens":8}'
curl http://127.0.0.1:8088/metrics
```

Configuration: `WARMSERVE_MAX_WORKERS` (2), `WARMSERVE_IDLE_TTL` (30s), `WARMSERVE_ADDR` (127.0.0.1:8088), `WARMSERVE_PYTHON` (python3), `WARMSERVE_WORKER` (worker.py).

An optional `WARMSERVE_UPSTREAM=http://127.0.0.1:8000` forwards the non-streaming `/v1/chat/completions` contract to an operator-configured server. The HTTP adapter is tested with a mock; **vLLM itself was not installed or exercised**. In this mode cold/warm metrics describe gateway slots, not external model loading, and eviction cannot stop a remote server. The demo model allowlist remains in force; adjust it explicitly for a real upstream. See the [vLLM serving documentation](https://docs.vllm.ai/en/latest/serving/openai_compatible_server/).

## Verify

```sh
go test -race -cover ./...
go vet ./...
.venv/bin/python benchmark.py
```

19 Go test functions cover reuse, cancellation, startup failure, LRU eviction, idle reclamation, backpressure, concurrent requests, malformed payloads, upstream failures, metrics and remote adapter configuration. Statement coverage was 64.7%; command startup/signal handling is not fully covered.

The benchmark builds and launches the gateway, launches real PyTorch processes, runs 12 concurrent requests, forces eviction and verifies idle reclamation. It writes `artifacts/benchmark.json`; one observed run is saved in `results/local-cpu.json`. On this machine: cold request 1102.18 ms, median of 10 warm requests 0.99 ms, 12/12 concurrent requests succeeded, worker count returned from 2 to 0. This tiny sample is not a production latency study or a meaningful language-model speed comparison.

## Deployment and next milestones

`Dockerfile` and `k8s/deployment.yaml` provide a non-root container and a single-replica deployment with resource requests, limits and health probes. **These templates were not executed: Docker was unavailable in the development environment.** Build `warmserve:local` and load it into your cluster before applying the manifest. There is no multi-replica coordinator or GPU scheduler.

Completed milestones: lifecycle-aware pool; bounded HTTP gateway; failure/concurrency tests; real-process benchmark. Next: measure a pretrained model on a GPU-backed vLLM server, separate queue/load/inference timing, add shared resource accounting and compare batching policies under controlled load. TensorRT, Triton, Ray, CUDA and training orchestration are not implemented. `ci/github-actions.yml` is an inactive workflow template; move it into `.github/workflows` to enable it.
