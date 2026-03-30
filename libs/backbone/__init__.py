"""Backbone fitting helpers."""

from libs.backbone.artifacts import (
    BackboneModel,
    BackboneSensorEnergy,
    BackboneSpec,
    build_backbone_artifacts_from_window_features_table,
)
from libs.backbone.energy import aggregate_sensor_energy_over_corpus, compute_window_sensor_energy
from libs.backbone.fit import (
    aggregate_backbone_gh,
    compute_backbone_gh_by_flight,
    reconstruct_window_vector,
    reconstruction_error,
    select_backbone_sensors_by_energy,
    select_backbone_sensors_by_energy_spark,
    solve_backbone_weights,
)
from libs.backbone.tables import (
    BackboneCrossTermFrame,
    BackboneGramFrame,
    BackboneSelectedSensorFrame,
    BackboneSensorEnergyTable,
    BackboneTable,
)

__all__ = [
    "aggregate_sensor_energy_over_corpus",
    "aggregate_backbone_gh",
    "BackboneModel",
    "BackboneSensorEnergy",
    "BackboneSensorEnergyTable",
    "BackboneSpec",
    "build_backbone_artifacts_from_window_features_table",
    "BackboneSelectedSensorFrame",
    "BackboneGramFrame",
    "BackboneCrossTermFrame",
    "BackboneTable",
    "compute_backbone_gh_by_flight",
    "compute_window_sensor_energy",
    "reconstruct_window_vector",
    "reconstruction_error",
    "select_backbone_sensors_by_energy",
    "select_backbone_sensors_by_energy_spark",
    "solve_backbone_weights",
]
