from libs.spark_sequence import segment_policy_from_env


def test_segment_policy_from_env_uses_defaults_when_env_missing(monkeypatch):
    monkeypatch.delenv("S3NTINEL_EVENT_SEGMENT_MAX_ROWS", raising=False)
    monkeypatch.delenv("S3NTINEL_EVENT_SEGMENT_MAX_SPAN_MS", raising=False)

    policy = segment_policy_from_env(
        "EVENT",
        default_max_rows_per_segment=50_000,
        default_max_span_ms=900_000,
    )

    assert policy.max_rows_per_segment == 50_000
    assert policy.max_span_ms == 900_000


def test_segment_policy_from_env_reads_overrides(monkeypatch):
    monkeypatch.setenv("S3NTINEL_WINDOW_SEGMENT_MAX_ROWS", "12345")
    monkeypatch.setenv("S3NTINEL_WINDOW_SEGMENT_MAX_SPAN_MS", "67890")

    policy = segment_policy_from_env(
        "window",
        default_max_rows_per_segment=50_000,
        default_max_span_ms=900_000,
    )

    assert policy.max_rows_per_segment == 12_345
    assert policy.max_span_ms == 67_890


def test_segment_policy_from_env_uses_profile_defaults(monkeypatch):
    monkeypatch.delenv("S3NTINEL_EVENT_SEGMENT_MAX_ROWS", raising=False)
    monkeypatch.delenv("S3NTINEL_EVENT_SEGMENT_MAX_SPAN_MS", raising=False)
    monkeypatch.setenv("S3NTINEL_SPARK_PROFILE", "laptop_large_sim_large_segments")

    policy = segment_policy_from_env(
        "EVENT",
        default_max_rows_per_segment=50_000,
        default_max_span_ms=900_000,
    )

    assert policy.max_rows_per_segment == 100_000
    assert policy.max_span_ms == 1_800_000
