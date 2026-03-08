# Simulation Codepath Wire Diagram

This note shows the current simulation execution seam as it exists in code.

It is intentionally **monolithic**:

- native V2.1 assembly authoring
- transitional legacy compilation
- behavior binding
- runtime construction
- module-local stepping
- inter-module propagation
- assembly-wide ordered tick
- downstream handoff into the fitting/profile pipeline

For the broader simulation design rationale, see
[simulation_architecture.md](/home/jrs/code/S3NTINEL/sentinel/docs/simulation_architecture.md).
For a focused view of latent-state and inter-module transfer, see
[simulation_coupling_wire_diagram.md](/home/jrs/code/S3NTINEL/sentinel/docs/simulation_coupling_wire_diagram.md).
For the downstream one-off fitting order after simulation emits telemetry, see
[fitting_workflow.md](/home/jrs/code/S3NTINEL/sentinel/docs/fitting_workflow.md).
For a standalone browser-rendered view of both diagrams, open
[simulation_wire_diagrams.html](/home/jrs/code/S3NTINEL/sentinel/docs/simulation_wire_diagrams.html).

## Diagram

```mermaid
flowchart TB
    subgraph A["Authoring And Compilation"]
        A1["Native authoring\nlibs/simulation/assembly.py\nHierarchyAssemblyBuilder\nbuild_hierarchy_assembly_spec(...)"]
        A2["Transitional legacy bridge\nlibs/simulation/compiler.py\nassembly_spec_from_hierarchy_spec(...)"]
        A3["Validation\nlibs/simulation/compiler.py\nvalidate_assembly_spec(...)"]
        A4["HierarchyAssemblySpec\nmodule_specs\ninter_module_couplings"]
        A1 --> A3
        A2 --> A3
        A3 --> A4
    end

    subgraph B["Behavior Resolution"]
        B1["Behavior registry\nlibs/behavior/registry.py\nbuild_default_behavior_registry()"]
        B2["Assembly binding\nlibs/simulation/binding.py\nbind_assembly_behaviors(...)"]
        B3["ModuleBehaviorBinding\nParameterBehaviorBinding"]
        B1 --> B2
        A4 --> B2
        B2 --> B3
    end

    subgraph C["Runtime Construction"]
        C0["AssemblyRuntime\nlibs/simulation/assembly_runtime.py\nAssemblyRuntime.from_spec(...)"]
        C1["ModuleRuntime\nlibs/simulation/runtime.py\nModuleRuntime.from_spec(...)"]
        C2["module_runtimes_from_specs(...)"]
        C3["Module runtime map\nmodule_id -> ModuleRuntime"]
        C4["Preindexed outgoing couplings\nsource_module_id -> couplings"]
        A4 --> C2
        C2 --> C3
        C1 --> C2
        A4 --> C0
        B3 --> C0
        C3 --> C0
        C0 --> C4
    end

    subgraph D["Per-Parameter Local Step Inputs"]
        D1["One BehaviorStepInput per parameter per tick\nlatent_state\ncontext\ndt_seconds"]
        D1A["resolve_step_inputs_for_module(...)\ndefault tick input when missing"]
        D2["Input port hydration\nlibs/simulation/module_step.py\ninject_input_ports_into_step_inputs(...)"]
        D3["Latent update hooks\nupdate_module_latent_state(...)"]
        D4["Runtime latent injection\ninject_runtime_latent_state_into_step_inputs(...)"]
        D1 --> D1A
        D1A --> D2
        D2 --> D3
        D3 --> D4
    end

    subgraph E["Module-Local Generation"]
        E1["ModuleStepRequest\nlibs/simulation/module_step.py"]
        E2["iter_module_samples(...)"]
        E3["Behavior generator\nparameter_behavior_binding.behavior.generator.generate_stream(...)"]
        E4["Optional violator\nbehavior.violator.violate_stream(...)"]
        E5["BehaviorSample stream\nparameter_value_clean\nparameter_value\nstate\nmetadata"]
        D4 --> E1
        B3 --> E1
        E1 --> E2
        E2 --> E3
        E3 --> E4
        E4 --> E5
    end

    subgraph F["Local Runtime Update"]
        F1["apply_module_sample_to_runtime(...)"]
        F2["ParameterRuntime.update_observation(...)"]
        F3["apply_module_sample_to_output_ports(...)"]
        F4["Output PortRuntime.current_value"]
        E5 --> F1
        F1 --> F2
        F1 --> F3
        F3 --> F4
    end

    subgraph G["Inter-Module Transfer"]
        G1["Outgoing InterModuleCouplingSpec\nsource_module_id/source_port_name\ntarget_module_id/target_port_name"]
        G2["propagate_inter_module_couplings(...)"]
        G3["apply_inter_module_coupling(...)"]
        G4["Drain due delayed transfers\nindependent of current gate"]
        G5["Phase gate / scoped mode gate check\nfor newly enqueued transfer"]
        G6["relation_type semantics\n drive / enable / inhibit"]
        G7["Target input PortRuntime.current_value"]
        F4 --> G2
        A4 --> G1
        G1 --> G2
        G2 --> G3
        G3 --> G4
        G4 --> G5
        G5 --> G6
        G6 --> G7
    end

    subgraph H["Assembly-Level Execution"]
        H1["AssemblyModuleStepRequest\nlibs/simulation/assembly_step.py"]
        H2["step_module_and_propagate(...)"]
        H3["AssemblyTickRequest\nlibs/simulation/orchestrator.py"]
        H4["step_assembly_once(...)"]
        H5["Caller-provided module_order"]
        H6["AssemblyRuntime.build_tick_request(...)"]
        D2 --> H1
        B3 --> H1
        C3 --> H1
        A4 --> H1
        H1 --> H2
        H3 --> H4
        H5 --> H4
        C3 --> H3
        B3 --> H3
        A4 --> H3
        C4 --> H3
        D1 --> H3
        C0 --> H6
        H6 --> H3
        H4 --> H2
        H2 --> E5
        H2 --> G2
    end

    subgraph I["Telemetry Output Boundary"]
        I1["Emitted parameter samples\nclean value + observed value"]
        I2["Telemetry rows for downstream use"]
        I3["Downstream one-off fitting sequence\n05_parameter_profiles_fit.py\n10_backbone_fit.py\n11_graph_fit.py"]
        I4["Inference consumers\nprofiler / detector / windows / scoring"]
        E5 --> I1
        I1 --> I2
        I2 --> I3
        I2 --> I4
    end
```

## Actual control flow in one ordered assembly tick

The current execution order is:

1. Build or compile a `HierarchyAssemblySpec`.
2. Validate module IDs and declared inter-module ports.
3. Resolve every `ParameterSpec.behavior_family_label` through the behavior registry.
4. Build `ModuleRuntime` objects from the validated module specs.
5. Build `AssemblyRuntime` as the precomputed execution context.
6. For each module in caller-provided `module_order`:
   - synthesize default one-tick step inputs for any bound parameter missing an explicit caller input
   - hydrate parameter step-input context from current input ports
   - update module-local latent state through `LatentUpdateSpec`
   - inject runtime latent state into each `BehaviorStepInput`
   - run the local behavior generator
   - optionally wrap the emitted stream through the behavior-local violator
   - write the module's emitted samples back to parameter runtime
   - write emitted clean values to output ports where declared
   - drain any due lagged transfers for outgoing couplings
   - enqueue and propagate any newly active inter-module couplings into downstream input ports
6. Return emitted samples grouped by `module_id`.

Main execution entrypoints:

- [assembly.py](/home/jrs/code/S3NTINEL/sentinel/libs/simulation/assembly.py)
- [assembly_runtime.py](/home/jrs/code/S3NTINEL/sentinel/libs/simulation/assembly_runtime.py)
- [binding.py](/home/jrs/code/S3NTINEL/sentinel/libs/simulation/binding.py)
- [runtime.py](/home/jrs/code/S3NTINEL/sentinel/libs/simulation/runtime.py)
- [module_step.py](/home/jrs/code/S3NTINEL/sentinel/libs/simulation/module_step.py)
- [transfer.py](/home/jrs/code/S3NTINEL/sentinel/libs/simulation/transfer.py)
- [assembly_step.py](/home/jrs/code/S3NTINEL/sentinel/libs/simulation/assembly_step.py)
- [orchestrator.py](/home/jrs/code/S3NTINEL/sentinel/libs/simulation/orchestrator.py)

## Semantics that are already locked

- `parameter_value_clean`
  - clean generator-side value before observation noise or perturbation
- `parameter_value`
  - observed downstream value consumed by profiler, detector, windowing, and scoring
- `behavior_family_label`
  - simulation-side nominal behavior assignment on the parameter spec
- `behavior_family_profiled`
  - downstream fitted artifact, not simulation runtime state
- module/assembly step inputs
  - one `BehaviorStepInput` per parameter per tick
  - if the caller omits one for a bound parameter, the orchestrator synthesizes a default single-tick input
  - stream semantics belong inside behavior generators and violators, not the assembly seam

## What this codepath intentionally does not do yet

The current seam is still deliberately minimal.

Not implemented in this execution path yet:

- iterative settling or multi-pass propagation
- global scheduler beyond one explicit ordered pass
- richer controller dynamics
- assembly-wide fault scheduling
- automatic use of downstream fitted artifacts during simulation
- fully typed/scoped execution of all coupling relation types beyond the current
  minimal `drive` / `enable` / `inhibit` semantics

Those belong to the next layer of V2.1 simulation work, not this initial codepath.
