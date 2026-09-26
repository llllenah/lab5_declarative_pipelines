# Declarative pipelines vs classic Spark jobs

| | Classic Spark job (Labs 3-4) | Declarative pipeline (Lakeflow, Lab 5) |
|---|---|---|
| Orchestration | I write the notebook order and dependencies myself (bronze notebook, then silver notebook, then a job with task dependencies) | The pipeline reads the dependency graph from which tables reference which, and runs them in the right order automatically |
| Schema handling | I inferred schema myself with `schema_of_json`, and explicitly fixed type issues | Same manual control still possible, but expectations + `apply_changes` remove a lot of the hand-written MERGE logic |
| Data quality | Checks (nulls, dedup) written as plain PySpark code, no built-in reporting | Declared as `@dp.expect*` constraints, with pass/fail counts shown in the pipeline UI automatically |
| Idempotency / reruns | Achieved manually via MERGE keyed on a business key | Achieved via `apply_changes` (SCD1/2 built in) or the pipeline's own incremental processing |
| Lineage | Not tracked unless I document it myself | Automatic, visible per-table in Catalog Explorer |
| Maintenance (OPTIMIZE/VACUUM) | I run it manually or schedule a separate job task | Not automatic inside the pipeline either — still needs a separate job task, same as the classic approach |
| Flexibility | Full control over every line of Spark code, easy to add arbitrary logic | Less flexible for anything outside the declared table/expectation model; workarounds needed for non-standard logic |
| Learning curve / debugging | Errors are plain Spark stack traces I'm used to | Errors come from the pipeline framework itself, and can be less familiar to debug at first |

## Cost considerations

A declarative pipeline runs on its own pipeline cluster (unless configured to share compute),
which is billed as pipeline compute rather than regular all-purpose/job compute. For a small
personal lab like this it's not a meaningful cost difference, but at team/production scale it's
worth knowing that pipeline runs are priced and monitored separately from regular job clusters.

## Bottom line for this lab

Declarative pipelines remove a lot of the boilerplate I wrote by hand in Lab 3-4 (dependency
ordering, lineage documentation, some of the MERGE logic for SCD2), at the cost of less direct
control over exactly how each step runs. For a small number of tables with fairly standard
bronze/silver/SCD2 logic, like this one, the trade is worth it; for anything with unusual
per-row logic, classic Spark still has the edge.
