# Hypothetical GIN design

## Scope

HypoPG models a GIN index well enough for PostgreSQL's planner to compare it
with other access paths. It does not build a GIN structure or attempt to
reproduce every detail of a physical build.

PostgreSQL's `gincostestimate()` already recognizes
`IndexOptInfo.hypothetical` and avoids opening a nonexistent index relation.
This behavior exists in every PostgreSQL version supported by this branch
(9.2 and later), so HypoPG uses the core cost estimator instead of maintaining
a private copy.

## Planner integration

GIN is admitted alongside the other supported access methods. The injected
`IndexOptInfo` contains HypoPG's estimates for `pages` and `tuples` and is
marked hypothetical. PostgreSQL 13 and later also receive a zero-initialized
`opclassoptions` array because their GIN cost code indexes that field.

GIN support is not restricted to PostgreSQL 17. The earlier restriction was a
HypoPG implementation guard, not a PostgreSQL planner requirement.

## Size estimation

GIN size depends on the keys returned by an operator class and their posting
lists, rather than directly on heap-row width. On PostgreSQL 9.5 and later,
HypoPG therefore:

1. Selects at most 1024 rows with deterministic `TABLESAMPLE SYSTEM ...
   REPEATABLE (0)`.
2. Applies a partial-index predicate and evaluates index expressions in the
   sample query.
3. Calls each selected GIN operator class's `extractValue` support function.
4. Sorts and deduplicates each row's keys using the operator-class comparator,
   or the key type's default B-tree comparator when the operator class does
   not supply one.
5. Measures posting entries, sampled unique keys, and actual key datum widths.
6. Converts those measurements into conservative entry-page and data-page
   estimates.

This is intentionally generic. Array, `tsvector`, `jsonb`, and extension
operator classes follow the same path; there are no per-operator-class size
constants copied from pull request 115.

The sample is capped at 100,000 extracted keys to bound planner memory and
latency. Its result is cached in the hypothetical-index object, so repeated
planning does not repeatedly scan the table.

## Security and error handling

The sample query runs as the relation owner with
`SECURITY_RESTRICTED_OPERATION` and a restricted search path. HypoPG is
temporarily disabled during nested SPI planning so the sample query cannot
recursively inject hypothetical indexes.

Sampling runs in an internal subtransaction. An expression, operator class, or
SPI failure is rolled back cleanly before HypoPG uses the fallback estimate.
The caller's user ID, security context, GUC nesting, HypoPG enabled state, and
EXPLAIN state are restored on both success and failure.

## Version policy and fallback

PostgreSQL 9.5 introduced `TABLESAMPLE`, so the generic sampler is compiled on
9.5 and later. PostgreSQL 9.2 through 9.4 still support hypothetical GIN, but
use the fallback estimator:

- Prefer a distinct-elements histogram for simple array or `tsvector`
  columns.
- Otherwise derive a conservative extracted-key count from average width.
- Assume fallback keys are unique, favoring an overestimate over an
  unrealistically cheap index.

The same fallback is used on newer versions when sampling is unavailable or
returns no usable keys. This keeps planning functional for unsupported custom
operator classes, inaccessible data, empty samples, and failing expressions.

Compatibility code remains localized with `PG_VERSION_NUM` guards:

- Sampling-only types and helpers: PostgreSQL 9.5 and later.
- Legacy `AllocSetContextCreate` arguments: PostgreSQL 9.5.
- `IndexOptInfo.opclassoptions`: PostgreSQL 13 and later.
- Existing HypoPG relation-open and statistics-slot compatibility paths are
  reused.

## Deliberate trade-offs

- The estimator is approximate and conservative. It does not model GIN page
  compression, posting-tree thresholds, pending-list state, or data
  distribution beyond the bounded sample.
- `SYSTEM` sampling operates on blocks and can miss rare values. A fixed
  repeatable seed favors stable planning and regression tests over repeated
  random exploration.
- Unique-key growth is extrapolated from the sample. It is not a full-table
  distinct estimator.
- Per-column custom operator-class options are not currently copied into the
  hypothetical index. Default and optionless extension operator classes are
  supported; option-sensitive extraction may fall back or differ from a real
  index.
- PostgreSQL 9.2 through 9.4 get planner support but not generic extraction
  sampling. Implementing an alternative random scan for those end-of-life
  releases would add complexity and potentially unbounded planning work.

Important index choices should still be validated with a representative
physical index before production deployment.

## Regression coverage

The GIN regression test covers:

- Array, `tsvector`, and `jsonb` operator classes.
- Planner selection and positive relation-size estimates.
- Expression and partial indexes.
- Different estimates for similarly sized JSONB values with different
  extracted-key density.
- Clean fallback after a sampled expression raises an error.

The sample-focused regression runs on PostgreSQL 9.5 and later. Older versions
are expected to compile the fallback-only path and should be included in the
build compatibility matrix.
