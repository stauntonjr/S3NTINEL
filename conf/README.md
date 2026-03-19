# Configuration

## Purpose

`conf/` contains checked-in baseline configuration assets used by the pipeline config layer and local demonstrations.

It owns:
- checked-in baseline defaults
- example maps and stream profiles

It does not own:
- environment-specific secrets
- operational deployment config
- per-run generated state

## How To Use

- `conf/defaults.yaml`
  - baseline defaults loaded by the typed pipeline config layer
- `conf/criticality_map.example.json`
  - example criticality mapping for telemetry or routing workflows
- `conf/demo_stream_profile.json`
  - example stream profile for demos or local experimentation

Environment variables override `defaults.yaml` through the typed loaders in `libs/config/`.

## Contents

- `defaults.yaml`
  - canonical repo defaults for pipeline stages and library behavior
- `criticality_map.example.json`
  - example only, not a required runtime file
- `demo_stream_profile.json`
  - demonstration input profile, not a persisted production artifact

## Data / Artifacts

`defaults.yaml` supplies base values for:
- pipeline stage controls
- graph/backbone/phase/scoring parameters
- algorithm tuning defaults

Artifact paths, write modes, and table formats are resolved by the runtime config layer:
- `libs/config/pipeline.py`
- `pipelines/common.py`

The repo favors:
- checked-in defaults here
- typed config access in pipeline stages
- runtime overrides through env vars at the boundary

## Subject Matter View

This directory is the repo’s baseline default configuration, not the full runtime configuration surface and not a substitute for per-environment deployment management.

## Notes / Constraints

- Keep example JSON files clearly marked as examples or demos.
- If a setting is routinely overridden by env vars, document the env var at the consuming stage or library README rather than duplicating all details here.
