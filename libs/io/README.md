# IO

## Purpose

`libs/io` owns artifact schemas, row contracts, and persistence/bridge utilities.

It does not own:
- domain algorithms
- simulation runtime
- stage orchestration

## How To Use

- Use `libs/io/schemas/*` for persisted artifact schemas.
- Use `libs/io/contracts.py` for in-memory row contracts.
- Use `delta.py`, `pandas_spark.py`, and `transforms.py` for IO-oriented helpers.

## Contents

- `schemas/`
  - persisted artifact schemas grouped by artifact domain
- `contracts.py`
  - in-memory row contracts
- `delta.py`
  - table persistence helpers
- `pandas_spark.py`
  - dataframe bridge utilities
- `transforms.py`
  - canonical input normalization and IO transformations

## Data / Artifacts

This package is the canonical home for:
- persisted Spark schema definitions
- ordered artifact column definitions
- IO contract boundaries between libraries and pipelines

## Subject Matter View

This package makes the telemetry-processing stack reproducible and interoperable across stages.

## Notes

- Keep Spark `StructType` definitions here, not scattered through domain packages.
