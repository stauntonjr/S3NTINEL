import importlib

from libs.cur.fit import resolve_u_contraction_mode


def test_resolve_u_contraction_mode_defaults_and_fallbacks():
    assert resolve_u_contraction_mode(None, has_a_matrix=True) == "pivot_restricted_a"
    assert resolve_u_contraction_mode("unknown_mode", has_a_matrix=True) == "pivot_restricted_a"
    assert resolve_u_contraction_mode("core_w", has_a_matrix=True) == "core_w"
    assert resolve_u_contraction_mode("full_a", has_a_matrix=True) == "full_a"
    assert resolve_u_contraction_mode("full_a", has_a_matrix=False) == "pivot_restricted_a"


def test_cur_report_propagates_u_contraction_mode():
    stage10 = importlib.import_module("pipelines.10_cur_backbone_fit")

    payload = stage10._build_cur_matrices_report(
        cur_column_sketch_path="a",
        cur_column_leverage_path="b",
        cur_row_sketch_path="c",
        cur_sensor_sample_path="d",
        cur_row_sample_path="e",
        cur_c_matrix_path="f",
        cur_r_matrix_path="g",
        cur_w_matrix_path="h",
        cur_u_matrix_path="i",
        column_sketch_count=1,
        column_leverage_count=1,
        row_sketch_count=1,
        cur_sampling_mode="deterministic",
        cur_sampling_seed=42,
        sampled_sensor_count=2,
        sampled_row_count=3,
        c_nnz=4,
        r_nnz=5,
        w_nnz=6,
        u_nnz=7,
        u_core_meta={"contraction_mode": "full_a"},
        effective_pivots_k=300,
        effective_row_samples_k=300,
        cur_u_contraction_mode="full_a",
    )

    assert payload["u_contraction_mode"] == "full_a"
    assert payload["u_core"]["contraction_mode"] == "full_a"
