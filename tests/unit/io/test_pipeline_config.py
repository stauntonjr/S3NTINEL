from __future__ import annotations

from libs.config.pipeline import load_pipeline_artifact_paths, load_pipeline_context_settings


def test_load_pipeline_artifact_paths_includes_lag_profile(monkeypatch):
    monkeypatch.setenv("S3NTINEL_LAG_PROFILE_TABLE_PATH", "/tmp/test-lag-profile")

    paths = load_pipeline_artifact_paths()

    assert paths.lag_profile == "/tmp/test-lag-profile"


def test_load_pipeline_context_settings_parses_lag_bands(monkeypatch):
    for env_name in (
        "S3NTINEL_V2_LAG_TAU_MAX_SECONDS",
        "S3NTINEL_V2_LAG_GRAPH_MIN_COUNT",
        "S3NTINEL_V2_LAG_GRAPH_MAX_MEAN_LAG_SECONDS",
        "S3NTINEL_V2_LAG_GRAPH_TOP_K_OUTGOING",
    ):
        monkeypatch.delenv(env_name, raising=False)

    settings = load_pipeline_context_settings(
        {
            "graph": {
                "lag": {
                    "tau_max_seconds": 120.0,
                    "min_count": 3,
                    "max_mean_lag_seconds": 45.0,
                    "top_k_outgoing": 5,
                    "bands": [
                        {"name": "quick", "lower_seconds": 0.0, "upper_seconds": 2.0, "combine_weight": 1.0},
                        {"name": "slow", "lower_seconds": 10.0, "upper_seconds": 30.0, "combine_weight": 0.5},
                    ],
                }
            }
        }
    )

    assert settings.graph.lag.tau_max_seconds == 120.0
    assert settings.graph.lag.min_count == 3
    assert settings.graph.lag.max_mean_lag_seconds == 45.0
    assert settings.graph.lag.top_k_outgoing == 5
    assert tuple(item.name for item in settings.graph.lag.bands) == ("quick", "slow")
    assert settings.graph.lag.bands[0].upper_seconds == 2.0
    assert settings.graph.lag.bands[1].combine_weight == 0.5
