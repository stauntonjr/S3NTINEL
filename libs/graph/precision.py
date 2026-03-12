from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class PrecisionGraphSpec:
    selected_sensors: tuple[str, ...]
    ridge_lambda: float = 1.0
    min_abs_partial_corr: float = 0.05


@dataclass(frozen=True)
class PrecisionGraph:
    spec: PrecisionGraphSpec
    edges: pd.DataFrame

    @classmethod
    def from_window_x(cls, window_x_df: pd.DataFrame, *, spec: PrecisionGraphSpec) -> PrecisionGraph:
        if not spec.selected_sensors or window_x_df.empty:
            return cls(spec=spec, edges=cls.empty_edges())

        rows: list[list[float]] = []
        for _, row in window_x_df.sort_values(["tail_id", "flight_id", "t_end", "win_id"], kind="mergesort").iterrows():
            scaled = row.get("continuous_vector_t_end_scaled")
            if not isinstance(scaled, dict):
                continue
            rows.append([float(scaled.get(parameter_name, 0.0) or 0.0) for parameter_name in spec.selected_sensors])

        if len(rows) < 2:
            return cls(spec=spec, edges=cls.empty_edges())

        x = np.asarray(rows, dtype=float)
        means = [float(sum(float(row[col_idx]) for row in x.tolist())) / float(len(rows)) for col_idx in range(len(spec.selected_sensors))]
        cov = np.zeros((len(spec.selected_sensors), len(spec.selected_sensors)), dtype=float)
        denom = float(max(len(rows) - 1, 1))
        for row in x.tolist():
            centered = [float(value) - means[idx] for idx, value in enumerate(row)]
            for i in range(len(spec.selected_sensors)):
                for j in range(len(spec.selected_sensors)):
                    cov[i, j] += centered[i] * centered[j]
        cov = cov / denom
        return cls.from_covariance(covariance=cov, spec=spec)

    @classmethod
    def from_covariance(cls, *, covariance: np.ndarray, spec: PrecisionGraphSpec) -> PrecisionGraph:
        if not spec.selected_sensors:
            return cls(spec=spec, edges=cls.empty_edges())
        if covariance.size == 0 or covariance.shape[0] != len(spec.selected_sensors):
            return cls(spec=spec, edges=cls.empty_edges())

        theta = cls._invert_small_matrix(
            covariance + (max(float(spec.ridge_lambda), 1e-6) * np.eye(covariance.shape[0], dtype=float))
        )
        out: list[dict[str, object]] = []
        for i, parameter_name_u in enumerate(spec.selected_sensors):
            for j in range(i + 1, len(spec.selected_sensors)):
                parameter_name_v = spec.selected_sensors[j]
                denom = max(theta[i, i] * theta[j, j], 1e-12) ** 0.5
                partial_corr = float(0.0 if denom <= 0 else (-theta[i, j] / denom))
                weight = abs(partial_corr)
                if weight < float(max(spec.min_abs_partial_corr, 0.0)):
                    continue
                out.append(
                    {
                        "parameter_name_u": parameter_name_u,
                        "parameter_name_v": parameter_name_v,
                        "partial_corr": partial_corr,
                        "precision_weight": weight,
                        "edge_family": "precision",
                    }
                )
        return cls(spec=spec, edges=pd.DataFrame(out, columns=cls.empty_edges().columns))

    @classmethod
    def from_window_x_spark(
        cls,
        window_x_df: "DataFrame",
        *,
        spec: PrecisionGraphSpec,
    ) -> PrecisionGraph:
        from pyspark.sql import functions as F

        backbone_sensors = [str(item) for item in spec.selected_sensors if str(item)]
        if not backbone_sensors:
            return cls(spec=spec, edges=cls.empty_edges())

        projection_exprs = [
            F.coalesce(
                F.element_at(F.col("continuous_vector_t_end_scaled"), F.lit(parameter_name)).cast("double"),
                F.lit(0.0),
            ).alias(f"x_{idx}")
            for idx, parameter_name in enumerate(backbone_sensors)
        ]
        projected = window_x_df.select(*projection_exprs)

        agg_exprs = [F.count(F.lit(1)).cast("long").alias("n")]
        for idx in range(len(backbone_sensors)):
            agg_exprs.append(F.sum(F.col(f"x_{idx}")).cast("double").alias(f"sum_{idx}"))
        for i in range(len(backbone_sensors)):
            for j in range(i, len(backbone_sensors)):
                agg_exprs.append((F.sum(F.col(f"x_{i}") * F.col(f"x_{j}")).cast("double")).alias(f"sum_{i}_{j}"))

        stats_row = projected.agg(*agg_exprs).collect()[0]
        row_count = int(stats_row["n"] or 0)
        if row_count < 2:
            return cls(spec=spec, edges=cls.empty_edges())

        means = [float(stats_row[f"sum_{idx}"] or 0.0) / float(row_count) for idx in range(len(backbone_sensors))]
        cov = np.zeros((len(backbone_sensors), len(backbone_sensors)), dtype=float)
        denom = float(max(row_count - 1, 1))
        for i in range(len(backbone_sensors)):
            for j in range(i, len(backbone_sensors)):
                cross_sum = float(stats_row[f"sum_{i}_{j}"] or 0.0)
                centered_sum = cross_sum - (float(row_count) * means[i] * means[j])
                cov_ij = centered_sum / denom
                cov[i, j] = cov_ij
                cov[j, i] = cov_ij
        return cls.from_covariance(covariance=cov, spec=spec)

    @staticmethod
    def _invert_small_matrix(matrix: np.ndarray) -> np.ndarray:
        n = int(matrix.shape[0])
        aug = np.concatenate([matrix.astype(float).copy(), np.eye(n, dtype=float)], axis=1)
        for pivot_idx in range(n):
            best_row = pivot_idx
            best_abs = abs(float(aug[pivot_idx, pivot_idx]))
            for row_idx in range(pivot_idx + 1, n):
                cand = abs(float(aug[row_idx, pivot_idx]))
                if cand > best_abs:
                    best_row = row_idx
                    best_abs = cand
            if best_row != pivot_idx:
                aug[[pivot_idx, best_row], :] = aug[[best_row, pivot_idx], :]
            pivot = float(aug[pivot_idx, pivot_idx])
            if abs(pivot) <= 1e-12:
                pivot = 1e-12
                aug[pivot_idx, pivot_idx] = pivot
            aug[pivot_idx, :] = aug[pivot_idx, :] / pivot
            for row_idx in range(n):
                if row_idx == pivot_idx:
                    continue
                factor = float(aug[row_idx, pivot_idx])
                if abs(factor) <= 1e-18:
                    continue
                aug[row_idx, :] = aug[row_idx, :] - (factor * aug[pivot_idx, :])
        return aug[:, n:]

    @staticmethod
    def empty_edges() -> pd.DataFrame:
        return pd.DataFrame(columns=["parameter_name_u", "parameter_name_v", "partial_corr", "precision_weight", "edge_family"])
