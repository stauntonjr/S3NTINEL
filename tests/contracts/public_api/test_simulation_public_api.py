from __future__ import annotations

import libs.simulation as simulation
import libs.simulation.runner as runner
from libs.simulation.aircraft.examples import build_coupled_module_aircraft_spec
from libs.simulation.flight.examples import build_named_flight_spec, list_flight_names


def _runner_config(tmp_path):
    return runner.PipelineRunConfig(
        flight_name="power_chain",
        tail_id="TPUB",
        flight_id="FPUB",
        n_steps=6,
        dt_seconds=1.0,
        base_dir=str(tmp_path),
        mode="full",
        table_format="parquet",
        write_mode="overwrite",
        min_warm=1,
        delta_threshold=0.0,
        slope_source="raw",
        ema_alpha=0.2,
        slope_threshold_mode="adaptive_run",
        slope_threshold_quantile=0.75,
        slope_threshold_scale=0.5,
        slope_threshold_min=1e-6,
        window_max_ms=10000,
        window_event_threshold=2,
        window_min_ms=50,
        window_inactivity_timeout_ms=0,
        window_strategy="segmented",
        phase_count=3,
        backbone_parameter_count=4,
        backbone_ridge_lambda=1.0,
        event_warmup_points=1,
    )


def test_libs_simulation_exports_curated_noun_surface_only():
    exported_names = {
        "Aircraft",
        "AircraftSpec",
        "Coupling",
        "CouplingSpec",
        "FaultProgramSpec",
        "FaultWindowSpec",
        "Fleet",
        "Flight",
        "FlightSpec",
        "FlightTick",
        "InitialStateSpec",
        "InputProgramSpec",
        "MisbehaviorProgram",
        "MisbehaviorProgramSpec",
        "MisbehaviorStepContext",
        "MisbehaviorWindowSpec",
        "LatentUpdate",
        "LatentSourceKind",
        "LatentUpdateSpec",
        "Module",
        "ModuleSpec",
        "Parameter",
        "ParameterSpec",
        "PhaseEnvelopeSpec",
        "PhaseProgram",
        "PhaseProgramSpec",
        "PhaseScheduleSpec",
        "PhaseSegmentSpec",
        "Port",
        "PortDirection",
        "PortSpec",
        "StepInputSpec",
        "Subsystem",
        "SubsystemSpec",
        "System",
        "SystemSpec",
        "Tail",
    }
    for exported_name in exported_names:
        assert hasattr(simulation, exported_name), exported_name

    removed_names = {
        "ModuleStepRequest",
        "bind_parameter_behavior",
        "bind_module_behaviors",
        "bind_assembly_behaviors",
        "InterModuleCouplingSpec",
        "regulate_coupling",
        "build_multibehavior_example",
        "build_multibehavior_flight",
        "build_named_flight",
        "list_flight_names",
        "phase_labels_to_table_df",
        "raw_telemetry_to_events_sdf",
        "raw_telemetry_to_window_features_sdf",
        "raw_telemetry_to_windows_sdf",
    }
    for removed_name in removed_names:
        assert not hasattr(simulation, removed_name), removed_name


def test_object_level_examples_are_public_from_subpackages():
    aircraft_spec = build_coupled_module_aircraft_spec()
    flight_names = list_flight_names()
    pressurization_flight = build_named_flight_spec("pressurization")

    assert aircraft_spec.aircraft_id == "coupled_module"
    assert "pressurization" in flight_names
    assert "power_pressurization_hierarchy_smoke" in flight_names
    assert "power_pressurization_hierarchy_medium" in flight_names
    assert pressurization_flight.metadata["flight_name"] == "pressurization"


def test_simulation_public_api_runs_end_to_end(tmp_path):
    result = runner.run_pipeline(_runner_config(tmp_path))

    assert result.status == "success"
    assert (result.paths.run_dir / "delta" / "raw_telemetry").exists()
    assert (result.paths.run_dir / "delta" / "parameter_event_profile").exists()
    assert (result.paths.run_dir / "delta" / "events").exists()
    assert (result.paths.run_dir / "delta" / "window_policy_profile").exists()
    assert (result.paths.run_dir / "delta" / "windows").exists()
    assert (result.paths.run_dir / "delta" / "window_scores_calibrated").exists()
    assert (result.paths.run_dir / "reports" / "phase_validation_summary.json").exists()
    assert (result.paths.run_dir / "reports" / "hierarchy_validation_summary.json").exists()
    assert (result.paths.run_dir / "reports" / "coupling_validation_summary.json").exists()
    assert (result.paths.run_dir / "reports" / "profile_validation_summary.json").exists()
    assert (result.paths.run_dir / "reports" / "event_validation_summary.json").exists()
    assert (result.paths.run_dir / "reports" / "label_contract_summary.json").exists()
    assert (result.paths.run_dir / "reports" / "score_validation_summary.json").exists()
    assert (result.paths.run_dir / "reports" / "misbehavior_window_validation_summary.json").exists()
    assert (result.paths.run_dir / "reports" / "fault_window_validation_summary.json").exists()
    assert (result.paths.run_dir / "reports" / "misbehavior_attribution_validation_summary.json").exists()
    assert (result.paths.run_dir / "reports" / "attribution_validation_summary.json").exists()
