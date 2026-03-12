# Anomaly Injection and Backbone Validation

This note records a practical research-backed view of two adjacent questions:

1. what anomaly injection methodologies fit the current S3NTINEL V2 architecture
2. how the backbone fit should be validated beyond "the code ran"

For the planned structured deviation ontology that should sit underneath anomaly
classification, see [misbehavior_taxonomy.md](/home/jrs/code/S3NTINEL/sentinel/docs/misbehavior_taxonomy.md).

The intent is not to catalogue every anomaly type in the literature. The intent is
to identify the families that are both scientifically defensible and operationally
compatible with the current code path.

For current implementation ownership, see:
- [libs/windows/README.md](/home/jrs/code/S3NTINEL/sentinel/libs/windows/README.md)
- [libs/backbone/README.md](/home/jrs/code/S3NTINEL/sentinel/libs/backbone/README.md)
- [libs/graph/README.md](/home/jrs/code/S3NTINEL/sentinel/libs/graph/README.md)
- [libs/phase/README.md](/home/jrs/code/S3NTINEL/sentinel/libs/phase/README.md)
- [libs/anomaly/README.md](/home/jrs/code/S3NTINEL/sentinel/libs/anomaly/README.md)

## 1. Scientific framing

Chandola, Banerjee, and Kumar's survey remains the cleanest generic taxonomy:
anomalies can be pointwise, contextual, or collective. That framing matters here
because S3NTINEL is explicitly not a pointwise thresholding system. It is a
windowed, graph-aware, phase-aware system. In this setting, the most useful
injections are the ones that disturb:

- local sensor values
- local dynamics within a window
- cross-sensor structure
- phase-conditioned behavior
- persistence across flights

Source:
- Chandola, Banerjee, Kumar, *Anomaly Detection: A Survey*  
  https://hdl.handle.net/11299/215731

## 2. Injection families that fit this system

### 2.1 Observation-space sensor faults

These are the basic sensor-fault families that are easiest to inject and easiest to
label precisely. They remain useful because they anchor the simulator to recognizable
failure modes.

The literature repeatedly returns to:

- bias / offset
- drift / incipient shift
- stuck-at
- spike / abrupt impulse
- saturation / clipping
- erratic / elevated noise
- data loss / dropout

Representative open descriptions:
- MDPI *Lightweight AI for Sensor Fault Monitoring*  
  https://www.mdpi.com/2079-9292/14/22/4532
- MDPI *Current Status and Prospects of Research on Sensor Fault Diagnosis of Agricultural Internet of Things*  
  https://www.mdpi.com/1424-8220/23/5/2528

For S3NTINEL, these should be implemented in two layers:

1. **sample-level perturbation**
   - bias
   - drift
   - spike
   - dropout
   - clipping
   - extra noise
2. **stream-level mode perturbation**
   - stuck-at over a contiguous interval
   - gain/scale change over an interval
   - delayed updates / frozen updates

These directly stress:

- event extraction
- window features
- reconstruction residuals

They should remain the first-line anomaly families because they produce crisp labels:

- onset time
- duration
- affected parameter
- anomaly type

### 2.2 Temporal and timing anomalies

For this system, timing anomalies are first-class. They are not an afterthought.
You already model events, windows, lag graphs, and transition graphs. That means the
simulator should inject faults that preserve value plausibility while breaking timing.

Useful timing anomalies:

- delayed response of one sensor to an upstream change
- phase lead/lag relative to a coupled sensor
- missed update intervals
- bursty updates inconsistent with nominal sampling rate
- asynchronous categorical transitions that should have been aligned

Why this matters:

- value-only anomalies mostly test residual scoring
- timing anomalies test:
  - lag graph
  - transition graph
  - graph-violation channel
  - event detector sequencing

This is especially relevant to mixed-rate telemetry, and even more so if the deferred
V2.1 rate-aware representation is adopted later.

### 2.3 Cross-sensor structural anomalies

This is where S3NTINEL can differentiate itself from a generic sensor-fault benchmark.

Inject anomalies that preserve univariate plausibility while breaking multivariate
structure:

- decouple two sensors that should co-vary
- invert sign of response within a causal chain
- change lag distribution between causally related sensors
- force an impossible or highly unlikely event transition
- create an unsupported same-window event co-occurrence

These are the right anomalies for a system that explicitly fits:

- `precision_graph`
- `event_graph`
- `lag_graph`
- `transition_graph`

They stress hierarchy and graph logic rather than only per-sensor residuals.

### 2.4 Phase-conditioned anomalies

An anomaly should not be treated as phase-agnostic if the underlying behavior is not.

Good contextual injections:

- a change that is anomalous only in one flight phase
- a categorical state that is legal in one phase and implausible in another
- a control response with wrong slope only during climb or descent
- abnormal variance during cruise but acceptable during takeoff or taxi

These are contextual anomalies in Chandola's sense, and they map directly to your
`phase_label` / `phase_*_detected` architecture.

### 2.5 Persistent degradation across flights

NASA's C-MAPSS remains the right mental model here: the fault is not always a short
localized interval. It may begin in a flight and then persist or worsen over the
remaining flights, effectively aging the system.

Source:
- NASA C-MAPSS dataset description  
  https://data.nasa.gov/dataset/c-mapss-aircraft-engine-simulator-data

For S3NTINEL, this suggests a distinct injection family:

- component degradation state that persists across flights for a tail
- slow parameter drift in health indicators
- persistent lag growth in causal couplings
- rising reconstruction residual on a subsystem over time

This is important because the current pipeline already has:

- `tail_id`
- `flight_id`
- per-tail phase baselines

So persistent degradation is not merely realistic; it is structurally compatible with
the current data model.

## 3. Recommended injection taxonomy for S3NTINEL

If the simulator is expanded, the clean taxonomy is:

### Level A: direct sensor faults

- `bias`
- `drift`
- `stuck`
- `spike`
- `noise`
- `dropout`
- `clipping`
- `gain_change`

### Level B: timing faults

- `lag_increase`
- `lag_jitter`
- `update_freeze`
- `update_burst`

### Level C: structural faults

- `coupling_break`
- `coupling_inversion`
- `unsupported_transition`
- `unsupported_cooccurrence`

### Level D: lifecycle faults

- `persistent_degradation`
- `phase_specific_degradation`

This taxonomy is better than a long flat list because it matches how the scoring
channels are organized:

- residual channel
- event channel
- graph channel
- phase channel

## 4. Backbone validation: what actually matters

The active backbone is a column-subset reconstruction model:

- choose sensors `C` by energy
- accumulate `G_f` and `H_f`
- solve `B = (G + λI)^(-1) H`

See:
- [libs/backbone/fit.py](/home/jrs/code/S3NTINEL/sentinel/libs/backbone/fit.py)

This is not generic PCA, so the validation should not be generic PCA boilerplate.

### 4.1 Held-out reconstruction error

This is the first non-negotiable validation.

The relevant theoretical target in column-based reconstruction is approximation of the
best low-rank reconstruction error, usually measured in Frobenius or spectral norm.

Source:
- Boutsidis, Drineas, Magdon-Ismail, *Near-Optimal Column-Based Matrix Reconstruction*  
  https://www.cs.purdue.edu/homes/pdrineas/documents/publications/Drineas_FOCS2011.pdf

For S3NTINEL, the practical version is:

1. split window-feature rows by flight or tail
2. fit `C` and `B` on train windows
3. compute held-out reconstruction error on validation windows
4. compare against:
   - smaller `k`
   - larger `k`
   - random sensor subsets
   - naive top-variance/top-energy baselines if different from the selected method

Primary metrics:

- mean held-out RMSE over all continuous sensors
- p95 held-out RMSE
- per-sensor held-out RMSE
- per-phase held-out RMSE

This should be done on `continuous_vector_t_end_scaled`, not on raw values.

### 4.2 Masked-sensor imputation error

Plain held-out-window error is necessary but not sufficient.

Because the backbone is specifically a subset-of-sensors model, one strong validation is:

1. mask some non-backbone sensors on held-out windows
2. reconstruct them from `x_C B`
3. measure prediction error only on the masked sensors

That tests what the backbone is actually supposed to do: infer the rest of the system
from selected sensors.

This is a better diagnostic than only looking at global residual norms.

### 4.3 Stability of selected sensors

Backbone selection should not be accepted if small resampling changes the selected
sensor set wildly.

This is where stability-selection thinking is useful, even if the exact estimator is not
lasso-based.

Sources:
- Meinshausen and Bühlmann, *Stability Selection*  
  https://academic.oup.com/jrsssb/article/72/4/417/7076513
- Shah and Samworth, *Variable Selection with Error Control: Another Look at Stability Selection*  
  https://www.repository.cam.ac.uk/handle/1810/245784

Recommended protocol:

1. subsample flights or tails
2. recompute backbone sensor set `C`
3. record selection frequency for each sensor
4. measure:
   - Jaccard overlap of selected sets
   - selection frequency per sensor
   - overlap stability as training window count increases

This is more meaningful for the current system than abstract feature-importance scores.

### 4.4 Rank/complexity validation

The chosen backbone size `k` should not be justified only by convenience.

Use:

- held-out reconstruction error curve vs `k`
- diminishing returns / elbow
- stability of selected `C` vs `k`

This is the correct analogue of model-order selection in PCA or factor models.
Cross-validation remains the standard principle for choosing model complexity.

Reference:
- Wold, *Cross-Validatory Estimation of the Number of Components in Factor and Principal Components Models*  
  https://doi.org/10.1080/00401706.1978.10489693

For S3NTINEL, the concrete rule should be:

- choose the smallest `k` such that held-out reconstruction error is near its plateau
- reject a `k` whose selected sensor set is unstable across resamples

### 4.5 Residual-subspace monitoring on nominal data

Even though the backbone is not PCA, process-monitoring logic still applies:

- the fitted backbone defines a "modeled" subspace
- residuals define an "unmodeled" subspace

The process-monitoring literature uses:

- Hotelling's `T^2` for modeled subspace variation
- `Q` / `SPE` for residual variation

Reference:
- Wang, Song, Li, *Fault Detection Behavior and Performance Analysis of Principal Component Analysis Based Process Monitoring Methods*  
  https://doi.org/10.1021/ie0007567

The S3NTINEL analogue is:

- on nominal windows, characterize the distribution of:
  - reconstruction error
  - per-sensor residuals
  - subsystem residual aggregates
- verify that injected anomalies shift these distributions materially

This should become part of routine validation, not just notebook analysis.

### 4.6 Downstream validation: does the backbone help the rest of the system?

The backbone is not an end in itself. It is only justified if it improves:

- phase separability
- hierarchy recovery
- anomaly scoring

So the practical validation ladder is:

1. held-out reconstruction quality
2. stability of `C`
3. downstream phase accuracy
4. downstream hierarchy recovery
5. anomaly score separation on injected anomalies

If step 1 is good but steps 3-5 do not improve, then the backbone is numerically fine
but systemically misaligned.

## 5. Recommended validation suite for the current repo

### Backbone fit validation suite

1. **Hold-out by flight**
   - fit on some flights, validate on held-out flights of the same tails
2. **Hold-out by tail**
   - fit on some tails, validate on unseen tails
3. **Masked-sensor reconstruction**
   - evaluate only on masked non-backbone sensors
4. **Stability under resampling**
   - repeated subsampling of flights/tails
5. **Error-vs-k curve**
   - reconstruction vs backbone size
6. **Phase-conditioned residual analysis**
   - reconstruction error broken down by `phase_label`

### Anomaly injection validation suite

1. **Direct sensor faults**
   - bias, drift, stuck, spike, noise, clipping, dropout
2. **Timing faults**
   - lag increase, lag jitter, frozen updates
3. **Structural faults**
   - broken coupling, inverted coupling, impossible transitions
4. **Persistent degradation**
   - cross-flight component aging
5. **Phase-conditioned faults**
   - only anomalous in selected phases

## 6. Recommendation

The next simulator upgrade should not aim for "more anomalies" in the abstract.
It should add a compact set of anomaly families that each illuminate one scoring channel:

- residual channel -> direct sensor faults
- lag/transition channel -> timing faults
- fused graph channel -> structural faults
- per-tail baseline channel -> persistent degradation
- phase channel -> contextual phase-conditioned faults

And the next backbone validation work should not start with more visualizations.
It should start with:

1. held-out reconstruction error
2. masked-sensor reconstruction
3. selection stability of `C`
4. downstream effect on phase/hierarchy/anomaly performance

That is the shortest path to deciding whether the backbone is merely elegant, or
actually useful.
