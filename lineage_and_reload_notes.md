# Lineage and safe reload

## Analyzing lineage

Two ways to check lineage for the pipeline's tables:

1. **Catalog Explorer UI**: open any table (`ticks_stream`, `cities_clean`,
   `cities_scd2`) and go to its **Lineage** tab. It shows the full upstream/downstream
   graph automatically, built from the pipeline's DAG, with no manual documentation needed.

2. **System tables** (SQL, for a query-based view):
   ```sql
   SELECT source_table_full_name, target_table_full_name, source_type
   FROM system.access.table_lineage
   WHERE target_table_full_name LIKE 'dbr_dev_ua5816bd.lena066636_%'
   ORDER BY event_time DESC;
   ```

This is one of the practical advantages of declarative pipelines over classic jobs: lineage is
derived from the pipeline graph automatically, instead of being something you'd have to document
by hand in a README.

## Reloading safely

Two kinds of reload in a declarative pipeline:

- **Full refresh**: drops and recomputes a table from scratch. Works cleanly for a
  `@dp.materialized_view` or a batch `@dp.table`, since they're fully recomputed anyway.
- **Incremental update**: the default "Start" button behavior; only processes new data since
  the last run.

**Watch out for append-only streaming tables.** A table created with `dp.create_streaming_table`
(like `cities_scd2` here, or any bronze table sourced from a stream) is marked
`pipelines.reset.allowed = false` by the framework by default, specifically to prevent a full
refresh from silently deleting streaming history. Trying a full refresh on it errors out
referencing that property. This is expected, not a bug. The safe way to reload such a table is:

1. Run an **incremental update** (no data loss, picks up only what changed), or
2. If a genuine full rebuild is needed, explicitly drop the table and let the pipeline
   recreate it, rather than fighting the `reset.allowed` guard.
