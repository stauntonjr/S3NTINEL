"""Phase decoding logic: assignment inputs, segmented decoding, and dwell enforcement."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from libs.perf.annotations import hot_path
from libs.phase.types import PhaseClusterModel, PhasePlanConfig
from libs.spark_sequence import SegmentedSequencePlan, SequenceOrderingPolicy
from libs.phase.utils import array_distance


@dataclass(frozen=True)
class PhaseSequenceState:
    score_column: str = "phase_scores"
    path_column: str = "phase_paths"
    initialized_column: str = "initialized"

    def initial_accumulator(self, *, phase_scores: "Column", phase_paths: "Column") -> "Column":
        from pyspark.sql import functions as F

        return F.struct(
            phase_scores.alias(self.score_column),
            phase_paths.alias(self.path_column),
            phase_scores.isNotNull().alias(self.initialized_column),
        )

    def aggregate_column(
        self,
        *,
        steps: "Column",
        phase_scores: "Column",
        phase_paths: "Column",
        transition_penalty: float,
    ) -> "Column":
        from pyspark.sql import functions as F

        penalty = F.lit(float(transition_penalty))
        huge = F.lit(1e18)

        def initial_state(step: "Column") -> "Column":
            indices = F.sequence(F.lit(1), F.size(step["phase_costs"]))
            return F.struct(
                F.transform(
                    indices,
                    lambda idx: F.element_at(step["phase_costs"], idx)
                    + F.when((idx - F.lit(1)) == step["hint_phase_id"], F.lit(0.0)).otherwise(F.lit(0.15)),
                ).alias(self.score_column),
                F.transform(indices, lambda idx: F.array(idx - F.lit(1))).alias(self.path_column),
                F.lit(True).alias(self.initialized_column),
            )

        def updated_state(acc: "Column", step: "Column") -> "Column":
            indices = F.sequence(F.lit(1), F.size(step["phase_costs"]))

            def stay_cost(idx: "Column") -> "Column":
                return F.element_at(acc[self.score_column], idx) + F.element_at(step["phase_costs"], idx)

            def transition_cost(idx: "Column") -> "Column":
                return F.when(
                    idx > F.lit(1),
                    F.element_at(acc[self.score_column], idx - F.lit(1)) + penalty + F.element_at(step["phase_costs"], idx),
                ).otherwise(huge)

            return F.struct(
                F.transform(indices, lambda idx: F.least(stay_cost(idx), transition_cost(idx))).alias(self.score_column),
                F.transform(
                    indices,
                    lambda idx: F.when(
                        (idx > F.lit(1)) & (transition_cost(idx) <= stay_cost(idx)),
                        F.concat(F.element_at(acc[self.path_column], idx - F.lit(1)), F.array(idx - F.lit(1))),
                    ).otherwise(F.concat(F.element_at(acc[self.path_column], idx), F.array(idx - F.lit(1)))),
                ).alias(self.path_column),
                F.lit(True).alias(self.initialized_column),
            )

        return F.aggregate(
            steps,
            self.initial_accumulator(phase_scores=phase_scores, phase_paths=phase_paths),
            lambda acc, step: F.when(~acc[self.initialized_column], initial_state(step)).otherwise(updated_state(acc, step)),
        )


def build_phase_distance_candidates(
    feature_df: "DataFrame",
    *,
    cluster_model: PhaseClusterModel,
) -> "DataFrame":
    from pyspark.sql import functions as F

    return (
        feature_df.join(cluster_model.centroids_df, on=["tail_id", "flight_id"], how="inner")
        .withColumn("raw_distance", array_distance(F.col("s_w_scaled"), F.col("s_w_centroid")))
        .join(
            cluster_model.distance_scales_df,
            on=["tail_id", "flight_id", "phase_id_detected"],
            how="left",
        )
        .withColumn("distance_scale", F.greatest(F.coalesce(F.col("distance_scale"), F.lit(1.0)), F.lit(1e-6)))
        .withColumn("phase_cost", F.col("raw_distance") / F.col("distance_scale"))
    )


def summarize_phase_candidates(distance_candidates_df: "DataFrame") -> "DataFrame":
    from pyspark.sql import functions as F

    candidate_struct = F.struct(
        F.col("phase_id_detected").alias("phase_id_detected"),
        F.col("raw_distance").alias("raw_distance"),
        F.col("distance_scale").alias("distance_scale"),
        F.col("phase_cost").alias("phase_cost"),
    )
    return (
        distance_candidates_df.groupBy(
            "tail_id",
            "flight_id",
            "win_id",
            "phase_row_number",
            "t_end",
            "effective_phase_count",
            "dwell_limit",
            "drift_threshold",
        )
        .agg(F.sort_array(F.collect_list(candidate_struct)).alias("phase_candidates"))
        .withColumn("phase_raw_distances", F.transform("phase_candidates", lambda candidate: candidate["raw_distance"]))
        .withColumn("phase_distance_scales", F.transform("phase_candidates", lambda candidate: candidate["distance_scale"]))
        .withColumn("phase_costs", F.transform("phase_candidates", lambda candidate: candidate["phase_cost"]))
        .withColumn(
            "raw_phase_pos",
            F.expr("cast(array_position(phase_raw_distances, array_min(phase_raw_distances)) as int)"),
        )
        .withColumn("raw_phase_id", (F.col("raw_phase_pos") - F.lit(1)).cast("int"))
        .withColumn(
            "raw_phase_confidence",
            F.greatest(
                F.lit(0.0),
                F.lit(1.0) - F.element_at(F.col("phase_costs"), F.col("raw_phase_pos")),
            ),
        )
        .drop("phase_candidates", "raw_phase_pos")
    )


def apply_phase_hint_smoothing(assignment_input_df: "DataFrame", *, config: PhasePlanConfig) -> "DataFrame":
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    smoothing_radius = max(int(config.phase_smoothing_radius), 0)
    if smoothing_radius > 0:
        order_window = Window.partitionBy("tail_id", "flight_id").orderBy("phase_row_number").rowsBetween(
            -smoothing_radius,
            smoothing_radius,
        )
        neighborhood_counts = [
            F.sum(F.when(F.col("raw_phase_id") == F.lit(idx), F.lit(1)).otherwise(F.lit(0))).over(order_window).cast("int")
            for idx in range(max(int(config.phase_count), 1))
        ]
        assignment_input_df = assignment_input_df.withColumn("phase_neighborhood_counts", F.array(*neighborhood_counts))
        return assignment_input_df.withColumn(
            "hint_phase_id",
            F.when(
                F.col("raw_phase_confidence") < F.lit(0.5),
                F.expr(
                    "cast(array_position(phase_neighborhood_counts, array_max(phase_neighborhood_counts)) - 1 as int)"
                ),
            ).otherwise(F.col("raw_phase_id")),
        ).drop("phase_neighborhood_counts")
    return assignment_input_df.withColumn("hint_phase_id", F.col("raw_phase_id"))


def _single_phase_assignments(assignment_input_df: "DataFrame") -> "DataFrame":
    from pyspark.sql import functions as F

    return assignment_input_df.where(F.col("effective_phase_count") <= F.lit(1)).withColumn(
        "phase_id_detected",
        F.lit(0).cast("int"),
    )


def _multi_phase_input(assignment_input_df: "DataFrame") -> "DataFrame":
    from pyspark.sql import functions as F

    return assignment_input_df.where(F.col("effective_phase_count") > F.lit(1))


def _build_segmented_phase_frame(
    multi_phase_input_df: "DataFrame",
    *,
    config: PhasePlanConfig,
) -> tuple[SegmentedSequencePlan, "DataFrame", "DataFrame"]:
    sequence_plan = SegmentedSequencePlan(
        ordering=SequenceOrderingPolicy(
            key_columns=("tail_id", "flight_id"),
            order_columns=("phase_row_number",),
            timestamp_column="t_end",
        ),
        policy=config.segment_policy,
    )
    segmented_frame = sequence_plan.assign_segments(
        multi_phase_input_df.select(
            "tail_id",
            "flight_id",
            "phase_row_number",
            "t_end",
            "phase_costs",
            "hint_phase_id",
        )
    )
    segment_steps_df = sequence_plan.build_segment_steps(
        segmented_frame.rows_df,
        step_columns=("phase_costs", "hint_phase_id"),
    )
    return sequence_plan, segmented_frame.segments_df, segment_steps_df


def _with_phase_carry(current_steps_df: "DataFrame", *, carry_df: "DataFrame | None") -> "DataFrame":
    from pyspark.sql import functions as F

    if carry_df is None:
        return (
            current_steps_df.withColumn("phase_scores", F.lit(None).cast("array<double>"))
            .withColumn("phase_paths", F.lit(None).cast("array<array<int>>"))
        )
    return current_steps_df.join(carry_df, on=["tail_id", "flight_id"], how="left")


def _advance_phase_segment(
    current_steps_df: "DataFrame",
    *,
    state: PhaseSequenceState,
    config: PhasePlanConfig,
) -> "DataFrame":
    return current_steps_df.withColumn(
        "phase_sequence_state",
        state.aggregate_column(
            steps=current_steps_df["steps"],
            phase_scores=current_steps_df["phase_scores"],
            phase_paths=current_steps_df["phase_paths"],
            transition_penalty=config.phase_transition_penalty,
        ),
    )


def _segment_carry(current_state_df: "DataFrame", *, state: PhaseSequenceState) -> "DataFrame":
    return current_state_df.select(
        "tail_id",
        "flight_id",
        current_state_df[f"phase_sequence_state.{state.score_column}"].alias("phase_scores"),
        current_state_df[f"phase_sequence_state.{state.path_column}"].alias("phase_paths"),
    )


def _final_phase_paths(carry_df: "DataFrame") -> "DataFrame":
    from pyspark.sql import functions as F

    return (
        carry_df.withColumn(
            "final_phase_pos",
            F.expr("cast(array_position(phase_scores, array_min(phase_scores)) as int)"),
        )
        .withColumn("phase_path", F.element_at(F.col("phase_paths"), F.col("final_phase_pos")))
        .select(
            "tail_id",
            "flight_id",
            F.posexplode_outer("phase_path").alias("pos", "phase_id_detected"),
        )
        .select(
            "tail_id",
            "flight_id",
            (F.col("pos") + F.lit(1)).cast("int").alias("phase_row_number"),
            F.col("phase_id_detected").cast("int").alias("phase_id_detected"),
        )
    )


@hot_path
def build_assignment_input(
    feature_df: "DataFrame",
    *,
    cluster_model: PhaseClusterModel,
    config: PhasePlanConfig,
) -> "DataFrame":
    assignment_input_df = summarize_phase_candidates(
        build_phase_distance_candidates(feature_df, cluster_model=cluster_model)
    )
    assignment_input_df = apply_phase_hint_smoothing(assignment_input_df, config=config)
    return assignment_input_df.select(
        "tail_id",
        "flight_id",
        "win_id",
        "phase_row_number",
        "t_end",
        "effective_phase_count",
        "dwell_limit",
        "drift_threshold",
        "phase_raw_distances",
        "phase_distance_scales",
        "phase_costs",
        "hint_phase_id",
    )


@hot_path
def assign_phases_segmented(assignment_input_df: "DataFrame", *, config: PhasePlanConfig) -> "DataFrame":
    from pyspark.sql import functions as F

    single_phase_assigned_df = _single_phase_assignments(assignment_input_df)
    multi_phase_input_df = _multi_phase_input(assignment_input_df)
    if not int(multi_phase_input_df.limit(1).count()):
        return single_phase_assigned_df

    sequence_plan, segments_df, segment_steps_df = _build_segmented_phase_frame(
        multi_phase_input_df,
        config=config,
    )
    state = PhaseSequenceState()
    carry_df = None
    for segment_id in sequence_plan.collect_segment_ids(segments_df):
        current_state_df = _advance_phase_segment(
            _with_phase_carry(
                segment_steps_df.where(F.col("flight_segment_id") == F.lit(int(segment_id))),
                carry_df=carry_df,
            ),
            state=state,
            config=config,
        )
        carry_df = _segment_carry(current_state_df, state=state)
    if carry_df is None:
        return assignment_input_df.limit(0)
    final_paths_df = _final_phase_paths(carry_df)
    multi_phase_assigned_df = multi_phase_input_df.join(
        final_paths_df,
        on=["tail_id", "flight_id", "phase_row_number"],
        how="inner",
    )
    return single_phase_assigned_df.unionByName(multi_phase_assigned_df)


def _phase_runs(assigned_df: "DataFrame") -> "DataFrame":
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    order_window = Window.partitionBy("tail_id", "flight_id").orderBy("phase_row_number")
    run_id_window = order_window.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    return (
        assigned_df.withColumn("prev_phase_id", F.lag("phase_id_detected").over(order_window))
        .withColumn(
            "phase_run_start_flag",
            F.when(F.col("prev_phase_id").isNull() | (F.col("prev_phase_id") != F.col("phase_id_detected")), F.lit(1)).otherwise(F.lit(0)),
        )
        .withColumn("phase_run_id", F.sum("phase_run_start_flag").over(run_id_window).cast("int"))
        .drop("prev_phase_id", "phase_run_start_flag")
    )


def _phase_run_stats(with_runs_df: "DataFrame") -> "DataFrame":
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    run_window = Window.partitionBy("tail_id", "flight_id").orderBy("run_start_row_number")
    return (
        with_runs_df.groupBy("tail_id", "flight_id", "phase_run_id")
        .agg(
            F.first("phase_id_detected").cast("int").alias("phase_id_detected"),
            F.first("dwell_limit").cast("int").alias("dwell_limit"),
            F.min("phase_row_number").cast("int").alias("run_start_row_number"),
            F.max("phase_row_number").cast("int").alias("run_end_row_number"),
            F.count(F.lit(1)).cast("int").alias("run_length"),
        )
        .withColumn("left_phase_id", F.lag("phase_id_detected").over(run_window))
        .withColumn("right_phase_id", F.lead("phase_id_detected").over(run_window))
    )


def _short_run_replacements(with_runs_df: "DataFrame", run_stats_df: "DataFrame") -> "DataFrame":
    from pyspark.sql import functions as F

    short_runs_df = run_stats_df.where(F.col("run_length") < F.col("dwell_limit"))
    if not int(short_runs_df.limit(1).count()):
        return short_runs_df.limit(0)
    return (
        with_runs_df.join(
            short_runs_df.select(
                "tail_id",
                "flight_id",
                "phase_run_id",
                F.col("phase_id_detected").alias("run_phase_id"),
                "left_phase_id",
                "right_phase_id",
            ),
            on=["tail_id", "flight_id", "phase_run_id"],
            how="inner",
        )
        .groupBy(
            "tail_id",
            "flight_id",
            "phase_run_id",
            "run_phase_id",
            "left_phase_id",
            "right_phase_id",
        )
        .agg(
            F.sum(
                F.when(
                    F.col("left_phase_id").isNotNull(),
                    F.element_at("phase_costs", F.col("left_phase_id") + F.lit(1)),
                ).otherwise(F.lit(0.0))
            ).cast("double").alias("left_cost"),
            F.sum(
                F.when(
                    F.col("right_phase_id").isNotNull(),
                    F.element_at("phase_costs", F.col("right_phase_id") + F.lit(1)),
                ).otherwise(F.lit(0.0))
            ).cast("double").alias("right_cost"),
        )
        .withColumn(
            "replacement_phase_id",
            F.when(
                F.col("left_phase_id").isNull() & F.col("right_phase_id").isNull(),
                F.lit(None).cast("int"),
            )
            .when(F.col("left_phase_id").isNull(), F.col("right_phase_id"))
            .when(F.col("right_phase_id").isNull(), F.col("left_phase_id"))
            .when(F.col("left_cost") <= F.col("right_cost"), F.col("left_phase_id"))
            .otherwise(F.col("right_phase_id")),
        )
        .where(F.col("replacement_phase_id").isNotNull() & (F.col("replacement_phase_id") != F.col("run_phase_id")))
    )


def _apply_phase_run_replacements(with_runs_df: "DataFrame", replacements_df: "DataFrame") -> "DataFrame":
    from pyspark.sql import functions as F

    return (
        with_runs_df.join(
            replacements_df.select(
                "tail_id",
                "flight_id",
                "phase_run_id",
                "replacement_phase_id",
            ),
            on=["tail_id", "flight_id", "phase_run_id"],
            how="left",
        )
        .withColumn(
            "phase_id_detected",
            F.coalesce(F.col("replacement_phase_id"), F.col("phase_id_detected")).cast("int"),
        )
        .drop("phase_run_id", "replacement_phase_id")
    )


def enforce_min_dwell(assigned_df: "DataFrame", *, config: PhasePlanConfig) -> "DataFrame":
    if max(int(config.phase_min_dwell_windows), 1) <= 1:
        return assigned_df

    current_df = assigned_df
    while True:
        with_runs_df = _phase_runs(current_df)
        replacements_df = _short_run_replacements(with_runs_df, _phase_run_stats(with_runs_df))
        if not int(replacements_df.limit(1).count()):
            return with_runs_df.drop("phase_run_id")
        current_df = _apply_phase_run_replacements(with_runs_df, replacements_df)


if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame
