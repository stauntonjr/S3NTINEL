# Operational Context: A-MATS, AFDX, And CBM+

## Purpose And Boundary

S3NTINEL is being developed to make high-dimensional aircraft signal feeds
actionable for condition-based maintenance. Its motivating operating premise is
that a target A-MATS-collected ARINC 664 Part 7 (AFDX) feed may expose roughly
30,000 heterogeneous parameters whose availability and meaning vary by aircraft,
flight, and configuration.

This is a design-rationale document, not a production integration contract. The
active V2 pipeline begins with normalized telemetry rows; it does not currently
decode AFDX frames, ingest from a live A-MATS deployment, integrate with BLADE,
or demonstrate capacity at the approximately 30,000-parameter target scale.
Current behavior remains owned by the [V2 architecture](../current/v2_architecture.md),
the [IO schemas](../../libs/io/schemas/README.md), package READMEs, and tests.

## CBM+ Need

The [Department of Defense CBM+ Guidebook](https://www.dau.edu/sites/default/files/2024-08/CBM%2B%20Guidebook%20August%202024%20-%20Stamped.pdf)
describes Condition-Based Maintenance Plus (CBM+) as a maintenance approach that
uses observed equipment condition, operating context, maintenance history, and
logistics context to support timely decisions. The goal is not merely to flag
unusual sensor values; it is to give maintainers and fleet-reliability teams
evidence that supports inspection, troubleshooting, maintenance planning, and
readiness decisions.

For an aircraft telemetry system, useful evidence must answer several questions:

- What changed, and how unusual was it in the current operating regime?
- Which parameters and related systems support that conclusion?
- Is the observation isolated, a persistent trend, or a coordinated structural
  change?
- What context and provenance should a maintainer review before acting?

Sensor-based analytics provides the first layer of this evidence: it profiles
parameter type and cadence, detects meaningful local changes, retains the
relevant observations, and measures data quality. Multivariate anomaly detection
adds the second layer: it tests whether the relationship among signals, events,
and operating context is unusual. This can surface distributed degradation or
inconsistent system behavior that no individual threshold captures. It does not,
by itself, diagnose a failed component or authorize maintenance action.

## A-MATS And BLADE Context

Public Air Force material describes A-MATS as the **Advanced Maintenance and
Troubleshooting Suite**, a commercial technology installed on aircraft to collect
large amounts of system data during flight and ground operations for aircrew,
maintainers, CBM+, and analysts. Its intended value is better access to aircraft
condition data for troubleshooting, health assessment, scheduling, and
forecasting. See the [AFSOC A-MATS announcement](https://www.afsoc.af.mil/News/Article-Display/Article/3219149/afsoc-spark-tank-finalists-to-compete-at-air-force-level/)
and the [Tesseract A-MATS portfolio entry](https://www.tesseract.af.mil/Portfolio_V3/Concept-Integrations/).

The archived A-MATS material records a progressive program shape rather than a
single finished deployment: aircraft-side collection, early constrained data
availability, staged fleet rollout, and a need to choose which data reaches the
shared analytics environment. It also records data-volume and cost concerns that
favor selective, governed analytical products over indiscriminate raw-data
replication. Those archive statements are historical planning context, not claims
about present program status or capacity.

In this context, BLADE is the Basing and Logistics Analytics Data Environment:
the broader logistics and analytics setting in which CBM+ evidence can be joined
with maintenance and supply context. S3NTINEL should therefore produce
traceable, bounded analytical artifacts that can be consumed alongside those
sources, rather than assume that BLADE is a source of raw AFDX frames. Public
background on BLADE and CBM+ is available in this [Air Force maintenance overview](https://www.airforcetimes.com/news/your-air-force/2022/02/14/us-air-force-fleets-mission-capable-rates-are-stagnating-heres-the-plan-to-change-that/).

## AFDX And The Analytics Implication

ARINC 664 Part 7, commonly called AFDX (Avionics Full-Duplex Switched Ethernet),
is a deterministic, switched-Ethernet avionics network. It uses configured
virtual links, traffic shaping, bandwidth allocation, and redundant paths to
make data transport predictable for safety-critical aircraft systems. The
[IEEE overview of AFDX virtual links](https://technav.ieee.org/topic/virtual-links/)
explains the transport role.

AFDX is adopted for modern integrated avionics because it provides a scalable,
high-throughput, deterministic network substrate in place of many isolated
point-to-point signal paths. It is not an analytics data model or a telemetry
schema. An analytics intake layer must still decode payloads, map signals to
stable parameter identities and units, preserve source or capture timestamps,
associate tail and flight context, and retain enough provenance to investigate a
result.

Deterministic transport also does not make all parameters synchronous or
comparable. Payloads may represent continuous measurements, discrete states,
commands, counters, or text-like status at different rates and under different
aircraft configurations. Redundant frames, packet loss, timing uncertainty,
configuration drift, and changing mission or flight phase remain analytical
concerns.

## Opportunities And Challenges

| Opportunity | Analytics challenge |
| --- | --- |
| Detect weak, coordinated changes before they become obvious single-parameter exceedances. | Do not confuse correlated behavior with causation or a confirmed fault. |
| Relate local parameter behavior to a system, subsystem, module, and operating phase. | Infer stable structure despite configuration variation, sparse observations, and changing regimes. |
| Reuse one captured signal feed for maintainers, CBM+ analysts, and fleet studies. | Preserve identity, units, timing, quality, and access controls from source through evidence. |
| Compare behavior across aircraft and flights. | Control for tail, software, loadout, mission, environmental, and phase differences. |
| Reduce a large signal set to an actionable review package. | Avoid dense all-pairs computation and opaque models at approximately 30,000 parameters. |

The approximately 30,000-parameter objective makes sparse, distributed
processing an architectural requirement. A dense correlation or feature matrix
over every parameter is not an acceptable default. Candidate selection, bounded
reference artifacts, partitioned Spark execution, and retained evidence are
needed to make analysis both tractable and reviewable.

## How The Active S3NTINEL Direction Meets The Need

The current pipeline direction addresses the analytical problem in layers:

1. **Normalize a stable analytical row.** Stage `00` accepts
   `tail_id`, `flight_id`, `timestamp_utc`, `parameter_name`,
   `parameter_value`, `unit`, and `rate_hz`, and forwards eligible
   noncanonical source columns. An AFDX adapter belongs upstream of this
   boundary; its packet and provenance contract remains to be designed.
2. **Respect rate and type.** Parameter and behavior profiling characterize
   datatype, observed cadence, scaling, and nominal behavior before structural
   fitting. Event extraction and adaptive windows retain meaningful changes
   without requiring every parameter to be densely resampled.
3. **Model relationships sparsely and by meaning.** The backbone, precision,
   event, lag, and transition graphs distinguish continuous conditional
   structure, co-presence, delayed relations, and sequence behavior. Their
   fused evidence supports a learned hierarchy rather than requiring a complete
   hand-authored system map.
4. **Condition anomaly evidence on operating state.** Phase fitting and
   phase-conditioned calibration reduce the risk of treating normal changes in
   flight regime as anomalous behavior.
5. **Deliver investigation evidence, not only a score.** Attribution emits a
   window identity, hierarchy context, ranked parameter telemetry, and event
   evidence so downstream users can inspect why a window was emitted.
6. **Keep the growing fact path distributed.** The active Spark boundary keeps
   telemetry, windows, and component-graph construction in Spark, collecting
   only bounded reference artifacts after pruning.

These choices make S3NTINEL directionally suited to the A-MATS/AFDX problem:
native-rate heterogeneous telemetry, a large parameter universe, changing
operating context, and a requirement for maintainer-facing evidence. They are
not yet proof of operational accuracy, integration readiness, or fleet-scale
throughput at the target parameter count.

## Notes

- A production AFDX decoder, A-MATS source adapter, BLADE integration contract,
  source-data governance policy, and approximately 30,000-parameter performance
  benchmark are future work. They are not part of the active V2 contract.
- The preferred public A-MATS expansion is "Advanced Maintenance and
  Troubleshooting Suite." Some later public material uses "System"; use the
  preferred project wording consistently unless an authoritative program source
  requires otherwise.
- For current artifact names and mathematical definitions, use the
  [glossary](../reference/glossary.md) and
  [theory foundations](../reference/theory_foundations.md), not historical
  architecture proposals.
