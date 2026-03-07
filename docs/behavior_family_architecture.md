# Behavior Family Architecture

This note refines the behavior-family mirror concept into a concrete package and class
layout.

The aim is to keep each behavior self-contained while still sharing common
protocols and registry infrastructure.

## 1. Package layout

Recommended shared package:

- `libs/behavior/`

Suggested initial files:

- `libs/behavior/base.py`
- `libs/behavior/registry.py`
- `libs/behavior/regulated.py`
- `libs/behavior/inertial.py`
- `libs/behavior/accumulative.py`
- `libs/behavior/discrete_state.py`
- `libs/behavior/derived_response.py`

This package is the common home for:

- the behavior contract
- generator / profiler / validator / violator components
- behavior-level feature extraction
- behavior-level expectations

Simulation and profiling can then depend on `libs/behavior` instead of each defining
their own disconnected behavior vocabulary.

## 2. One file per family

Recommended rule:

- one file per behavior
- one main behavior container class per file

That file should contain:

- behavior identity
- behavior contract metadata
- feature extraction logic
- nominal generator logic
- profiling logic
- validation hooks
- violation logic

This keeps all semantics for a behavior in one place.

## 3. Shared base/protocols

Put only shared protocols and lightweight value objects in `base.py`.

Suggested contents:

- `Behavior`
- `BehaviorGenerator`
- `BehaviorProfiler`
- `BehaviorValidator`
- `BehaviorViolator`
- `BehaviorFeatureExtractor`
- `BehaviorContract`
- `BehaviorProfileResult`
- `BehaviorExpectation`

These should be protocols or small dataclasses, not heavy base classes.

## 4. Main per-family object

Each behavior file should expose one main class.

Example:

- `RegulatedBehavior`
- `InertialBehavior`
- `AccumulativeBehavior`
- `DiscreteStateBehavior`
- `DerivedResponseBehavior`

That class should compose the family pieces instead of exposing many unrelated
top-level classes.

## 5. Suggested internal composition

Each behavior class can own these components:

- `contract`
- `feature_extractor`
- `generator`
- `profiler`
- `validator`
- `violator`
- `expectation`

This makes the mirror explicit:

- generator and profiler are paired
- validator checks the behavior contract
- violator introduces family-specific deviations
- expectation validates the pairing

## 6. Suggested interfaces

### behavior contract

```python
@dataclass(frozen=True)
class BehaviorContract:
    behavior_family: str
    expected_traits: tuple[str, ...]
    supported_datatypes: tuple[str, ...]
    allowed_fault_families: tuple[str, ...]
```

### feature extractor

```python
class BehaviorFeatureExtractor(Protocol):
    def compute_features(
        self,
        *,
        parameter_name: str,
        telemetry_pdf: pd.DataFrame,
    ) -> dict[str, float | str | None]:
        ...
```

### generator

```python
class BehaviorGenerator(Protocol):
    def step(
        self,
        *,
        dt_seconds: float,
        latent_state: Mapping[str, float],
        parameter_state: Any,
        context: Mapping[str, Any],
    ) -> Any:
        ...

    def observe(
        self,
        *,
        parameter_state: Any,
        context: Mapping[str, Any],
    ) -> object:
        ...
```

### profiler

```python
class BehaviorProfiler(Protocol):
    def profile(
        self,
        *,
        parameter_name: str,
        features: Mapping[str, float | str | None],
    ) -> "BehaviorProfileResult":
        ...
```

### profile result

```python
@dataclass(frozen=True)
class BehaviorProfileResult:
    behavior_family_profiled: str
    behavior_profile_confidence: float
    score_by_family: Mapping[str, float]
    profiled_features: Mapping[str, float | str | None]
```

### validator

```python
class BehaviorValidator(Protocol):
    def validate(
        self,
        *,
        generated_rows: pd.DataFrame,
        profile_result: "BehaviorProfileResult",
    ) -> dict[str, float | bool | str]:
        ...
```

### violator

```python
class BehaviorViolator(Protocol):
    def violate(
        self,
        *,
        parameter_name: str,
        generated_rows: pd.DataFrame,
        context: Mapping[str, Any],
    ) -> pd.DataFrame:
        ...
```

### expectation

```python
class BehaviorExpectation(Protocol):
    def evaluate(
        self,
        *,
        generated_rows: pd.DataFrame,
        profile_result: BehaviorProfileResult,
    ) -> dict[str, float | bool | str]:
        ...
```

## 7. Example family skeleton

### `libs/behavior/regulated.py`

Recommended main object:

```python
class RegulatedBehavior(Behavior):
    contract: BehaviorContract
    feature_extractor: BehaviorFeatureExtractor
    generator: BehaviorGenerator
    profiler: BehaviorProfiler
    validator: BehaviorValidator
    violator: BehaviorViolator
    expectation: BehaviorExpectation
```

Expected responsibilities:

- define the canonical family name: `regulated`
- define the expected traits:
  - `bounded`
- `central_band_occupancy`
- `mean_reverting`
- define the nominal generator behavior
- define the profiling heuristics
- define the family-specific violator logic
- define minimum expected self-classification behavior

### `libs/behavior/inertial.py`

Recommended main object:

```python
class InertialBehavior(Behavior):
    ...
```

Expected responsibilities:

- define the canonical family name: `inertial`
- define expected traits:
  - `smooth`
  - `persistent`
  - `lagged_response`
- define the nominal latent-response generator
- define autocorrelation / smoothness-oriented detection heuristics
- define the inertial-specific violator logic

## 8. Registry

Use a registry object to centralize discovery and lookup.

Suggested file:

- `libs/behavior/registry.py`

Suggested class:

- `BehaviorRegistry`

Responsibilities:

- register behaviors by canonical name
- return one behavior by name
- iterate all behaviors
- support validation/test loops

Example:

```python
registry = BehaviorRegistry(
    behaviors=[
        RegulatedBehavior(),
        InertialBehavior(),
        AccumulativeBehavior(),
        DiscreteStateBehavior(),
    ]
)
```

Useful methods:

- `get(name: str) -> Behavior`
- `all() -> tuple[Behavior, ...]`
- `names() -> tuple[str, ...]`

## 9. Integration points

### simulation

Simulation should use the behavior registry to:

- assign `behavior_family_label`
- attach the correct generator
- optionally attach behavior-level expectations for validation fixtures

### profiling

Profiling should use the behavior registry to:

- compute behavior-oriented features
- score the observed parameter against each behavior
- choose `behavior_family_profiled`

### testing

Tests should use the registry to:

- generate nominal behavior traces
- run detection on those traces
- verify contract expectations

Suggested test file:

- `tests/test_behavior_contracts.py`

## 10. Naming recommendations

Prefer:

- `Behavior`
- `BehaviorContract`
- `BehaviorProfileResult`
- `BehaviorRegistry`
- `RegulatedBehavior`

Avoid vaguer names like:

- `BehaviorModel`
- `FamilyThing`
- `BehaviorUtils`
- `Contracts`

The names should make the local mirrored role obvious.

## 11. First implementation scope

If implemented incrementally, start with:

- `base.py`
- `registry.py`
- `regulated.py`
- `inertial.py`

That is enough to prove the pattern.

Then add:

- `accumulative.py`
- `discrete_state.py`

Defer:

- `derived_response.py`

until the heuristics are sharper.

## 12. Recommendation

Use:

- one file per behavior family
- one composed family class per file
- shared protocols in `base.py`
- registry-based discovery in `registry.py`

That gives you the cleanest structure for the generator / profiler / validator /
violator mirror without fragmenting the concept across too many packages.
