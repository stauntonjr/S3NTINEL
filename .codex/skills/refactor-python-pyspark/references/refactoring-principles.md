# Refactoring Principles

Use these principles when deciding whether to move code, introduce a dataclass, or redesign a PySpark flow.

## Core Rules

1. Keep one implementation.
   Delete local-only, test-only, or alternate copies unless a real runtime distinction is required.

2. Model meaningful state with dataclasses.
   If a group of primitives describes one domain entity, represent it as one dataclass instead of parallel parameters, ad hoc dicts, or loosely coordinated locals.

3. Use dataclass properties for derived state.
   Prefer properties for values that are computed from object state and conceptually belong to the object, including Spark `Column` expressions when they are intrinsic to the model.

4. Prefer direct construction.
   Instantiate objects directly or expose classmethods such as `from_row`, `from_config`, or `from_frame_schema` when alternate sources are needed. Avoid third-party or caller-managed builders unless the existing codebase already depends on them heavily.

5. Put state-coupled transformations on the object.
   If a transformation depends on the object's fields, make it an instance method unless another class has clearer ownership.

6. Attach behavior to the most natural class.
   Choose method placement by domain meaning, not convenience. If a method reads like "a window defines its bounds" or "a metric selects its columns", place it there.

7. Let the domain drive names and boundaries.
   Use names that reflect the represented business objects, pipeline stages, or physical analogies that make the flow easier to reason about.

8. Respect the current codebase before adding structure.
   First look for the smallest coherent change: extend an existing class, introduce one focused dataclass, or collapse duplicate helpers before creating new modules or abstraction layers.

9. Keep the hot path in PySpark.
   Assume production-scale workloads. Favor built-in Spark expressions, joins, aggregations, and window functions over Python loops, `.collect()`, row-wise logic, or local mirrors of Spark behavior.

10. Prioritize algorithmic efficiency.
    Fix bad joins, repeated scans, unnecessary shuffles, and poor state representation before adding architectural polish.

11. Represent Spark logic coherently through domain objects.
    It is acceptable and often preferable for a domain dataclass to hold metadata fields, expose `Column` properties, and implement `DataFrame` transformation methods when that keeps the flow explicit and inspectable.

12. Keep tests synchronized with the code's model.
    Update tests when names, boundaries, or objects change. Tests should reflect the same domain taxonomy as production code and should validate behavior, not freeze accidental structure.

13. Update documentation after verification.
    Once tests or equivalent checks pass, update affected documentation so it describes the current names, objects, boundaries, and behavior rather than the pre-refactor structure.

14. Optimize for reasoning simplicity.
    Prefer consistent names, shared paths, and compact code. Avoid thin wrappers that only rename arguments or shuttle state between functions.

## Preferred Patterns

Use the patterns below when the refactor genuinely clarifies domain meaning and removes duplication. Do not introduce these shapes mechanically.

## Replace Primitive State

Instead of:

```python
def build_score(sensor_u: str, sensor_v: str, weight: float) -> Column:
    ...
```

Prefer:

```python
@dataclass(frozen=True)
class SensorPair:
    sensor_u: str
    sensor_v: str
    weight: float

    @property
    def score_column(self) -> Column:
        ...
```

## Move Behavior to the Domain Object

Instead of free functions that shuttle several related arguments:

```python
def add_features(df: DataFrame, start_col: str, end_col: str, window_size: int) -> DataFrame:
    ...
```

Prefer:

```python
@dataclass(frozen=True)
class TimeWindow:
    start_col: str
    end_col: str
    window_size: int

    def add_features(self, df: DataFrame) -> DataFrame:
        ...
```

## Keep Spark Expressions Close to the Model

Use properties when the expression is a stable part of the object's meaning:

```python
@dataclass(frozen=True)
class MetricSpec:
    raw_col: str
    baseline_col: str

    @property
    def delta(self) -> Column:
        return F.col(self.raw_col) - F.col(self.baseline_col)
```

## Collapse Duplicate Paths

If test code and production code perform the same transform, keep one implementation and make the test call the real path. Only split the paths when the runtime contract genuinely differs.

## Keep Tests in Service of the Code

When the production taxonomy improves, update the tests to match it. Do not preserve misleading test helper names, fixture shapes, or fake object boundaries just to avoid touching test code. A good test suite validates the real behavior and uses the same domain language as the implementation.

## Review Questions

- Does each new class represent a real domain concept?
- Did the refactor remove duplication or just move it?
- Could any new helper become a method on an existing class instead?
- Did any Spark logic move out of Spark unnecessarily?
- Did the tests move with the production taxonomy and object model?
- Did the documentation move after the verified code and tests?
- Are the object names the ones a domain expert would use?
- Did the change reduce lines rather than expand them?
- Did the change increase conceptual definition and clarity rather than decrease them?
