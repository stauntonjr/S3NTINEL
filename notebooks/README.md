# Notebooks

Use this directory for exploratory analysis and validation notebooks.

- `simulation_exploration.ipynb` is the visualization-only notebook for inspecting completed simulation run bundles, reports, validations, logs, and the emitted `explorer_bundle`.

## Notebook workflow

- Run notebook code in the registered `sentinel-spark35` Jupyter kernel, not in Codex sandboxed execution.
- Use Codex for targeted notebook help, debugging, and refactors, not as the notebook runtime.
- Keep chat context narrow: share the failing cell, traceback, minimal upstream state, and only the relevant helper code.
- Prefer pointing Codex at the notebook or workspace files instead of pasting large notebook dumps into chat.

## Bind the conda env to a notebook kernel

Create the environment first if it does not already exist:

```bash
conda env create -f environment.spark35.yml
```

Activate the environment, install `ipykernel`, and register the kernel:

```bash
conda activate sentinel-spark35
python -m pip install ipykernel
python -m ipykernel install --user --name sentinel-spark35 --display-name "Python (sentinel-spark35)"
```

If the kernel does not appear immediately in VS Code, reopen the window or refresh the available kernels.

## VS Code

- Select the `sentinel-spark35` Python interpreter for the workspace when relevant.
- Select the `Python (sentinel-spark35)` kernel from the notebook kernel picker before running notebook cells.
- If the selected interpreter or kernel differs from the expected env, trust the notebook kernel selection over Codex sandbox execution.

The active architecture is V2. Notebook analysis should follow:

- `window_x` for backbone fit inputs
- `window_s` for phase/scoring structure
- V2 fitting artifacts (`backbone`, `precision_graph`, `event_graph`, `lag_profile`, `lag_graph`, `transition_graph`, `fused_graph`, `hierarchy_sensor_map`)
  - `lag_profile` is the first-class per-band lag artifact
  - `lag_graph` is the collapsed compatibility view consumed by downstream fusion
- canonical label/detected naming only

Removed notebooks should be recreated only from active pipeline artifacts.

Notebook cleanup rules:

- prefer `parameter_datatype_label` and `parameter_datatype_profiled`
- prefer `event_type_label` and `event_type_detected`
- do not use `truth_*`
- do not treat `cooccur` as part of the active V2 event contract
