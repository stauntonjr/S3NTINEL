# Performance and Run Metadata

## Purpose

`libs/perf` owns operational instrumentation helpers:
- MLflow integration
- wall-time logging
- memory observability snapshots
- stage-manifest generation
- lightweight logging annotations

## How To Use

- Apply the decorators and helpers from this package in stage entrypoints and run orchestration code.
- Keep business logic out of this package.

## Contents

- `annotations.py`
  - common instrumentation decorators
- `memory.py`
  - driver and Spark memory instrumentation helpers
- `mlflow.py`
  - MLflow helpers and local summary emission
- `stage_manifest.py`
  - manifest generation for persisted artifacts
- `logger.py`
  - runtime logging helpers

## Data / Artifacts

This package emits:
- stage manifests
- grouped run summaries
- MLflow parameters and metrics
- optional memory snapshot artifacts via `S3NTINEL_OBSERVABILITY_MEMORY_*`

## Notes

- This package is operational infrastructure, not a domain package.
