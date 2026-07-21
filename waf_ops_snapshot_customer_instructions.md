# Customer Run Instructions: WAF Ops Snapshot Notebook

Please run `waf_ops_snapshot_report_notebook.py` in the Databricks workspace where the WAF Light Tooling has already been installed and reloaded.

Import it as a Databricks notebook source file:

1. Go to **Workspace**.
2. Choose **Import**.
3. Upload `waf_ops_snapshot_report_notebook.py`.
4. Open the imported notebook, attach a cluster or serverless notebook compute, set the widgets, and run all cells.

Before running the snapshot, confirm the WAF reload job has completed at least once. A quick check is:

```sql
SELECT *
FROM <waf_catalog>.waf_cache._run_log
ORDER BY run_id DESC
LIMIT 5;
```

## What this notebook reads

It reads the WAF Light Tooling outputs from:

```text
<waf_catalog>.waf_cache
```

The key objects it expects are:

```text
_run_log
waf_total_percentage_across_pillars
waf_controls_g
waf_controls_c
waf_controls_p
waf_controls_r
waf_recommendations_not_met
```

## What this notebook creates

It creates:

- A self-contained HTML presentation for ops / executive walkthrough.
- A CSV action plan export.
- Optional dashboard-ready Delta tables:

```text
<waf_catalog>.waf_cache.waf_ops_snapshot_metadata
<waf_catalog>.waf_cache.waf_ops_snapshot_pillar_scores
<waf_catalog>.waf_cache.waf_ops_snapshot_controls
<waf_catalog>.waf_cache.waf_ops_snapshot_recommendations
<waf_catalog>.waf_cache.waf_ops_snapshot_action_plan
```

## Parameters to set before running

| Widget | Suggested value |
|---|---|
| `waf_catalog` | The catalog used when WAF Light Tooling was installed, for example `main` |
| `waf_schema` | Usually `waf_cache` |
| `customer_name` | Friendly name for the customer/environment |
| `assessment_label` | Presentation title, for example `Production WAF Ops Snapshot` |
| `top_n_recommendations` | Usually `15` or `20` |
| `output_dir` | `dbfs:/FileStore/waf_ops_snapshot` or a UC Volume path |
| `write_snapshot_tables` | `true` if you want dashboard-ready tables, `false` if read-only reporting is required |

## Permissions needed

The user running the notebook needs:

- `USE CATALOG` on the WAF catalog.
- `USE SCHEMA` on the WAF schema.
- `SELECT` on the WAF cache tables/views.
- If `write_snapshot_tables=true`: permission to create/overwrite tables in the WAF schema.
- Write permission to the chosen `output_dir`.

## Output to send back

After the notebook completes, please send back:

- The generated HTML file path or browser URL printed by the notebook.
- The action-plan CSV folder path.
- If `write_snapshot_tables=true`, confirmation that the `waf_ops_snapshot_*` tables were created.
- A screenshot of the final "Overall WAF Completion" section if direct file sharing is blocked.

If the notebook fails, please send the full error output plus the values used for `waf_catalog`, `waf_schema`, and `output_dir`.
