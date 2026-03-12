# IO Schemas

## Purpose

This directory contains the persisted artifact schema definitions for the repo.

## Contents

- `telemetry.py`
- `events.py`
- `windows.py`
- `backbone.py`
- `graph.py`
- `phase.py`
- `scoring.py`
- `anomaly.py`
- `profiling.py`

Each module owns:
- ordered artifact columns
- Spark schema definitions
- artifact-specific schema helpers where needed

## Notes

- Persisted table schemas belong here, not in domain runtime modules.
- In-memory row contracts live in `libs/io/contracts.py`.
