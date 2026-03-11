# Gossip Protocol Simulation Framework

This repository is based on https://github.com/libp2p/test-plans/tree/master/gossipsub-interop.

## Prerequisites

- [Shadow](https://shadow.github.io/docs/guide/supported_platforms.html) simulator
- [uv](https://docs.astral.sh/uv/) (Python package manager)
- Go toolchain (for GossipSub)
- Rust toolchain (for DOG)

## Building binaries

```sh
make binaries
```

## Running a simulation

```sh
uv run run.py --protocol dog --scenario random --network uniform
```

### Required arguments

| Flag | Description |
|------|-------------|
| `--protocol` | `dog` or `gossipsub` |
| `--scenario` | Scenario name (see below) |

### Optional arguments

| Flag | Default | Description |
|------|---------|-------------|
| `--network` | — | Network topology (`uniform`, `binary`, `real`, `faivre`, `med-bandwidth`, `low-bandwidth`) |
| `--seed` | `1` | Random seed for reproducibility |
| `--parallelism` | `24` | Number of threads used by Shadow |
| `--output-dir` | auto-generated | Output directory for simulation data |
| `--dry-run` | off | Generate config files without running Shadow |

### Available scenarios

- `random` — 1000 nodes, random mesh, moderate traffic
- `two-cliques` — 1000 nodes split into two dense cliques
- `faivre-30-tps` — 32 nodes, 30 TPS sustained load
- `malicious-new-connections` — Honest + malicious DOG nodes
- `rolling-churn` — Nodes join/leave over time

## Re-parsing existing simulation data

To re-parse logs and regenerate plots from an existing simulation run:

```sh
uv run analyze_logs.py -d <output-dir>
```

To regenerate plots from already-parsed data (skipping log parsing):

```sh
uv run analyze_logs.py -d <output-dir> --use-existing-data
```

| Flag | Default | Description |
|------|---------|-------------|
| `-d`, `--dir` | (required) | Path to simulation output directory |
| `-o`, `--output-dir` | `data` | Subdirectory for parsed data (relative to `--dir`) |
| `-w`, `--warmup-time` | `120` | Warmup seconds to exclude from plots |
| `--use-existing-data` | off | Skip log parsing, use previously parsed data |

## Output structure

```
<output-dir>/
  params/          # Per-node JSON config files
  phases.json      # Phase metadata (active nodes and messages per phase)
  graph.gml        # Network topology graph
  shadow.yaml      # Shadow configuration
  shadow.data/     # Raw Shadow simulation output
  data/            # Structured data generated from log parsing
    messages.json    # Message send/delivery timestamps
    snapshots/       # Periodic bandwidth snapshots
  plots/           # Generated plots
```
