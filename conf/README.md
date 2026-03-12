# Configuration

## Purpose

`conf/` contains default and example configuration assets used by the persisted pipeline and local demonstrations.

It owns:
- baseline pipeline defaults
- example maps and stream profiles

It does not own:
- environment-specific secrets
- operational deployment config
- per-run generated state

## How To Use

- `conf/defaults.yaml`
  - default stage and library settings used by `pipelines/common.py`
- `conf/criticality_map.example.json`
  - example criticality mapping for telemetry or routing workflows
- `conf/demo_stream_profile.json`
  - example stream profile for demos or local experimentation

Environment variables override many `defaults.yaml` settings at runtime.

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
- output-path defaults where the pipeline context supports them

The repo favors:
- checked-in defaults here
- runtime overrides through env vars

## Subject Matter View

This directory is the repo’s baseline operating configuration, not a substitute for per-environment deployment management.

## Notes / Constraints

- Keep example JSON files clearly marked as examples or demos.
- If a setting is routinely overridden by env vars, document the env var at the consuming stage or library README rather than duplicating all details here.
