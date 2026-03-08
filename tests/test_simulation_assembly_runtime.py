from libs.simulation import (
    AssemblyRuntime,
    build_native_coupled_module_example,
)


def test_assembly_runtime_builds_bindings_runtimes_and_coupling_index():
    assembly_spec = build_native_coupled_module_example()
    assembly_runtime = AssemblyRuntime.from_spec(assembly_spec)

    assert set(assembly_runtime.module_bindings_by_id) == {"MOD_SOURCE", "MOD_TARGET"}
    assert set(assembly_runtime.module_runtimes_by_id) == {"MOD_SOURCE", "MOD_TARGET"}
    assert assembly_runtime.module_order == ("MOD_SOURCE", "MOD_TARGET")
    assert set(assembly_runtime.outgoing_inter_module_couplings_by_source_module) == {"MOD_SOURCE"}


def test_assembly_runtime_build_tick_request_reuses_precomputed_context():
    assembly_spec = build_native_coupled_module_example()
    assembly_runtime = AssemblyRuntime.from_spec(assembly_spec)
    tick_request = assembly_runtime.build_tick_request(step_inputs_by_module={})

    assert tick_request.module_bindings_by_id is assembly_runtime.module_bindings_by_id
    assert tick_request.module_runtimes_by_id is assembly_runtime.module_runtimes_by_id
    assert tick_request.module_order == assembly_runtime.module_order
    assert (
        tick_request.outgoing_inter_module_couplings_by_source_module
        is assembly_runtime.outgoing_inter_module_couplings_by_source_module
    )
