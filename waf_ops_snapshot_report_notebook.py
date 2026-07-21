# Databricks notebook source
# MAGIC %md
# MAGIC # WAF Ops Snapshot Report
# MAGIC
# MAGIC Run this notebook after the customer has installed and run
# MAGIC [Databricks WAF Light Tooling](https://github.com/AbhiDatabricks/Databricks-WAF-Light-Tooling).
# MAGIC
# MAGIC It reads the generated `waf_cache` tables and creates:
# MAGIC
# MAGIC - An executive / operations HTML presentation
# MAGIC - Delta snapshot tables that can be used for an AI/BI dashboard
# MAGIC - A prioritized action plan from the WAF "Not Met" recommendations
# MAGIC
# MAGIC The notebook is read-only against the WAF tool outputs. It writes only to the configured output directory and optional snapshot tables in the same schema.

# COMMAND ----------

from datetime import datetime, timezone
from html import escape
import json
import re

from pyspark.sql import functions as F
from pyspark.sql import types as T

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Parameters

# COMMAND ----------

dbutils.widgets.text("waf_catalog", "main", "WAF catalog")
dbutils.widgets.text("waf_schema", "waf_cache", "WAF schema")
dbutils.widgets.text("customer_name", "Customer", "Customer / environment name")
dbutils.widgets.text("assessment_label", "Databricks Well-Architected Snapshot", "Report title")
dbutils.widgets.text("top_n_recommendations", "15", "Top recommendations to show")
dbutils.widgets.text("output_dir", "dbfs:/FileStore/waf_ops_snapshot", "Output directory")
dbutils.widgets.dropdown("write_snapshot_tables", "true", ["true", "false"], "Write snapshot Delta tables")

WAF_CATALOG = dbutils.widgets.get("waf_catalog").strip()
WAF_SCHEMA = dbutils.widgets.get("waf_schema").strip()
CUSTOMER_NAME = dbutils.widgets.get("customer_name").strip() or "Customer"
ASSESSMENT_LABEL = dbutils.widgets.get("assessment_label").strip() or "Databricks Well-Architected Snapshot"
OUTPUT_DIR = dbutils.widgets.get("output_dir").strip().rstrip("/")
WRITE_SNAPSHOT_TABLES = dbutils.widgets.get("write_snapshot_tables").lower() == "true"

try:
    TOP_N = max(1, int(dbutils.widgets.get("top_n_recommendations")))
except Exception:
    TOP_N = 15

SNAPSHOT_TS = datetime.now(timezone.utc)
SNAPSHOT_ID = SNAPSHOT_TS.strftime("%Y%m%dT%H%M%SZ")

print("WAF Ops Snapshot parameters")
print(f"  catalog              : {WAF_CATALOG}")
print(f"  schema               : {WAF_SCHEMA}")
print(f"  customer_name        : {CUSTOMER_NAME}")
print(f"  assessment_label     : {ASSESSMENT_LABEL}")
print(f"  top_n_recommendations: {TOP_N}")
print(f"  output_dir           : {OUTPUT_DIR}")
print(f"  write_snapshot_tables: {WRITE_SNAPSHOT_TABLES}")
print(f"  snapshot_id          : {SNAPSHOT_ID}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Helpers

# COMMAND ----------

def qident(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def fqtn(table: str) -> str:
    return f"{qident(WAF_CATALOG)}.{qident(WAF_SCHEMA)}.{qident(table)}"


def table_exists(table: str) -> bool:
    try:
        spark.sql(f"DESCRIBE TABLE {fqtn(table)}").limit(1).collect()
        return True
    except Exception:
        return False


def table_columns(table: str):
    try:
        return set(spark.table(fqtn(table)).columns)
    except Exception:
        return set()


def read_table(table: str):
    return spark.table(fqtn(table))


def scalar_or_none(sql: str):
    rows = spark.sql(sql).limit(1).collect()
    return rows[0][0] if rows else None


def collect_dicts(df, limit=None):
    if limit:
        df = df.limit(limit)
    return [r.asDict(recursive=True) for r in df.collect()]


def safe_float(value, default=0.0):
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def safe_int(value, default=0):
    try:
        if value is None:
            return default
        return int(value)
    except Exception:
        return default


def normalize_status(value) -> str:
    s = (value or "").strip().lower()
    if s in ("met", "pass", "passed", "yes", "true"):
        return "Met"
    if s in ("not met", "fail", "failed", "no", "false"):
        return "Not Met"
    return value or "Unknown"


def severity_from_gap(score, threshold):
    score_f = safe_float(score)
    threshold_f = safe_float(threshold)
    gap = max(threshold_f - score_f, 0.0)
    if gap >= 50 or score_f < 25:
        return "Critical"
    if gap >= 25:
        return "High"
    if gap > 0:
        return "Medium"
    return "Low"


def timeframe_for_severity(sev):
    return {
        "Critical": "0-30 days",
        "High": "0-30 days",
        "Medium": "30-60 days",
        "Low": "60-90 days",
    }.get(sev, "30-60 days")


def owner_for_pillar(pillar):
    p = (pillar or "").lower()
    if "governance" in p:
        return "Data governance / platform owner"
    if "cost" in p:
        return "FinOps / platform owner"
    if "performance" in p:
        return "Data engineering / platform owner"
    if "reliability" in p:
        return "Platform operations / workload owner"
    return "Platform owner"


def get_workspace_context():
    ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    out = {}
    for key, getter in {
        "user": lambda: ctx.userName().get(),
        "notebook_path": lambda: ctx.notebookPath().get(),
        "workspace_host": lambda: ctx.browserHostName().get(),
        "workspace_id": lambda: ctx.workspaceId().get(),
    }.items():
        try:
            out[key] = getter()
        except Exception:
            out[key] = ""
    return out


CTX = get_workspace_context()
print(json.dumps(CTX, indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Validate WAF Outputs

# COMMAND ----------

expected_tables = [
    "_run_log",
    "waf_total_percentage_across_pillars",
    "waf_controls_g",
    "waf_controls_c",
    "waf_controls_p",
    "waf_controls_r",
    "waf_recommendations_not_met",
]

table_status = [(t, table_exists(t)) for t in expected_tables]
display(spark.createDataFrame(table_status, "table_name string, exists boolean"))

missing_core = [t for t, ok in table_status if not ok and t != "waf_recommendations_not_met"]
if missing_core:
    raise Exception(
        "Missing required WAF tables/views: "
        + ", ".join(missing_core)
        + f". Confirm the WAF Light Tooling reload job has run and that {WAF_CATALOG}.{WAF_SCHEMA} is correct."
    )

# COMMAND ----------

latest_run = {}
if table_exists("_run_log"):
    latest_run_rows = collect_dicts(
        spark.sql(
            f"""
            SELECT run_id, triggered_at, finished_at, status, tables_succeeded, tables_failed
            FROM {fqtn("_run_log")}
            ORDER BY run_id DESC
            LIMIT 1
            """
        )
    )
    latest_run = latest_run_rows[0] if latest_run_rows else {}

print("Latest WAF reload run")
print(json.dumps({k: str(v) for k, v in latest_run.items()}, indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Build Snapshot Data

# COMMAND ----------

# Pillar score table. Prefer the WAF summary view if it has the full rollup shape;
# otherwise fall back to individual pillar totals. This protects customers on older
# or locally modified WAF Light Tooling versions.
summary_cols = table_columns("waf_total_percentage_across_pillars")
required_summary_cols = {"pillar", "total_controls", "implemented_controls", "completion_percent"}

if table_exists("waf_total_percentage_across_pillars") and required_summary_cols.issubset(summary_cols):
    pillar_scores_df = read_table("waf_total_percentage_across_pillars")
else:
    if table_exists("waf_total_percentage_across_pillars"):
        print(
            "waf_total_percentage_across_pillars does not expose total_controls and "
            "implemented_controls; falling back to per-pillar total tables."
        )
    parts = []
    for pillar, table in [
        ("Data & AI Governance", "waf_total_percentage_g"),
        ("Cost Optimization", "waf_total_percentage_c"),
        ("Performance Efficiency", "waf_total_percentage_p"),
        ("Reliability", "waf_total_percentage_r"),
    ]:
        if table_exists(table):
            parts.append(
                read_table(table)
                .select(
                    F.lit(pillar).alias("pillar"),
                    F.col("total_controls"),
                    F.col("implemented_controls"),
                    F.col("completion_percent"),
                )
            )
    if not parts:
        raise Exception("Could not find WAF pillar score tables.")
    pillar_scores_df = parts[0]
    for part in parts[1:]:
        pillar_scores_df = pillar_scores_df.unionByName(part, allowMissingColumns=True)

pillar_scores_df = (
    pillar_scores_df
    .select(
        F.col("pillar").cast("string").alias("pillar"),
        F.col("total_controls").cast("int").alias("total_controls"),
        F.col("implemented_controls").cast("int").alias("implemented_controls"),
        F.col("completion_percent").cast("double").alias("completion_percent"),
    )
    .withColumn("snapshot_id", F.lit(SNAPSHOT_ID))
    .withColumn("snapshot_ts_utc", F.lit(SNAPSHOT_TS.strftime("%Y-%m-%d %H:%M:%S")))
)

display(pillar_scores_df.orderBy("completion_percent"))

# COMMAND ----------

control_parts = []

if table_exists("waf_controls_g"):
    control_parts.append(
        read_table("waf_controls_g").select(
            F.lit("Data & AI Governance").alias("pillar"),
            F.col("waf_id").cast("string"),
            F.col("principle").cast("string"),
            F.col("description").cast("string").alias("best_practice"),
            F.col("score_percentage").cast("double"),
            F.col("threshold_percentage").cast("double"),
            F.col("threshold_met").cast("string"),
            F.col("implemented").cast("string"),
        )
    )

for pillar, table in [
    ("Cost Optimization", "waf_controls_c"),
    ("Performance Efficiency", "waf_controls_p"),
    ("Reliability", "waf_controls_r"),
]:
    if table_exists(table):
        control_parts.append(
            read_table(table).select(
                F.lit(pillar).alias("pillar"),
                F.col("waf_id").cast("string"),
                F.col("principle").cast("string"),
                F.col("best_practice").cast("string"),
                F.col("score_percentage").cast("double"),
                F.col("threshold_percentage").cast("double"),
                F.col("threshold_met").cast("string"),
                F.col("implemented").cast("string"),
            )
        )

if not control_parts:
    raise Exception("No WAF control tables found.")

controls_df = control_parts[0]
for part in control_parts[1:]:
    controls_df = controls_df.unionByName(part, allowMissingColumns=True)

normalize_status_udf = F.udf(normalize_status, T.StringType())
severity_udf = F.udf(severity_from_gap, T.StringType())
timeframe_udf = F.udf(timeframe_for_severity, T.StringType())
owner_udf = F.udf(owner_for_pillar, T.StringType())

controls_df = (
    controls_df
    .withColumn("threshold_met", normalize_status_udf(F.col("threshold_met")))
    .withColumn("gap_to_threshold", F.greatest(F.col("threshold_percentage") - F.col("score_percentage"), F.lit(0.0)))
    .withColumn("severity", severity_udf(F.col("score_percentage"), F.col("threshold_percentage")))
    .withColumn("suggested_timeframe", timeframe_udf(F.col("severity")))
    .withColumn("suggested_owner", owner_udf(F.col("pillar")))
    .withColumn("snapshot_id", F.lit(SNAPSHOT_ID))
    .withColumn("snapshot_ts_utc", F.lit(SNAPSHOT_TS.strftime("%Y-%m-%d %H:%M:%S")))
)

display(controls_df.orderBy(F.desc("gap_to_threshold"), "pillar", "waf_id"))

# COMMAND ----------

if table_exists("waf_recommendations_not_met"):
    recommendations_df = (
        read_table("waf_recommendations_not_met")
        .select(
            F.col("waf_id").cast("string"),
            F.col("pillar_name").cast("string").alias("pillar"),
            F.col("principle").cast("string"),
            F.col("best_practice").cast("string"),
            F.col("score_percentage").cast("double"),
            F.col("control_threshold_pct").cast("double").alias("threshold_percentage"),
            F.col("recommendation_if_not_met").cast("string").alias("recommendation"),
        )
    )
else:
    recommendations_df = (
        controls_df
        .where(F.col("threshold_met") == "Not Met")
        .select(
            "waf_id",
            "pillar",
            "principle",
            "best_practice",
            "score_percentage",
            "threshold_percentage",
            F.lit("Review this WAF control and use the Databricks WAF recommendation catalog for detailed remediation.").alias("recommendation"),
        )
    )

recommendations_df = (
    recommendations_df
    .withColumn("gap_to_threshold", F.greatest(F.col("threshold_percentage") - F.col("score_percentage"), F.lit(0.0)))
    .withColumn("severity", severity_udf(F.col("score_percentage"), F.col("threshold_percentage")))
    .withColumn("suggested_timeframe", timeframe_udf(F.col("severity")))
    .withColumn("suggested_owner", owner_udf(F.col("pillar")))
    .withColumn("snapshot_id", F.lit(SNAPSHOT_ID))
    .withColumn("snapshot_ts_utc", F.lit(SNAPSHOT_TS.strftime("%Y-%m-%d %H:%M:%S")))
)

top_recommendations_df = recommendations_df.orderBy(
    F.expr("CASE severity WHEN 'Critical' THEN 1 WHEN 'High' THEN 2 WHEN 'Medium' THEN 3 ELSE 4 END"),
    F.desc("gap_to_threshold"),
    "pillar",
    "waf_id",
).limit(TOP_N)

display(top_recommendations_df)

# COMMAND ----------

action_plan_df = (
    recommendations_df
    .select(
        "suggested_timeframe",
        "severity",
        "suggested_owner",
        "pillar",
        "waf_id",
        "principle",
        "best_practice",
        "score_percentage",
        "threshold_percentage",
        "gap_to_threshold",
        "recommendation",
        "snapshot_id",
        "snapshot_ts_utc",
    )
    .orderBy(
        F.expr("CASE suggested_timeframe WHEN '0-30 days' THEN 1 WHEN '30-60 days' THEN 2 ELSE 3 END"),
        F.expr("CASE severity WHEN 'Critical' THEN 1 WHEN 'High' THEN 2 WHEN 'Medium' THEN 3 ELSE 4 END"),
        F.desc("gap_to_threshold"),
    )
)

display(action_plan_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Optional Snapshot Tables for Dashboarding

# COMMAND ----------

if WRITE_SNAPSHOT_TABLES:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {qident(WAF_CATALOG)}.{qident(WAF_SCHEMA)}")

    (
        pillar_scores_df.write.mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(fqtn("waf_ops_snapshot_pillar_scores"))
    )
    (
        controls_df.write.mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(fqtn("waf_ops_snapshot_controls"))
    )
    (
        recommendations_df.write.mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(fqtn("waf_ops_snapshot_recommendations"))
    )
    (
        action_plan_df.write.mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(fqtn("waf_ops_snapshot_action_plan"))
    )

    meta_schema = T.StructType([
        T.StructField("snapshot_id", T.StringType(), False),
        T.StructField("snapshot_ts_utc", T.StringType(), False),
        T.StructField("customer_name", T.StringType(), True),
        T.StructField("assessment_label", T.StringType(), True),
        T.StructField("waf_catalog", T.StringType(), True),
        T.StructField("waf_schema", T.StringType(), True),
        T.StructField("workspace_host", T.StringType(), True),
        T.StructField("workspace_id", T.StringType(), True),
        T.StructField("notebook_user", T.StringType(), True),
        T.StructField("latest_run_id", T.StringType(), True),
        T.StructField("latest_run_status", T.StringType(), True),
        T.StructField("latest_run_finished_at", T.StringType(), True),
    ])
    meta_df = spark.createDataFrame([(
        SNAPSHOT_ID,
        SNAPSHOT_TS.strftime("%Y-%m-%d %H:%M:%S"),
        CUSTOMER_NAME,
        ASSESSMENT_LABEL,
        WAF_CATALOG,
        WAF_SCHEMA,
        CTX.get("workspace_host", ""),
        str(CTX.get("workspace_id", "")),
        CTX.get("user", ""),
        str(latest_run.get("run_id", "")),
        str(latest_run.get("status", "")),
        str(latest_run.get("finished_at", "")),
    )], meta_schema)
    (
        meta_df.write.mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(fqtn("waf_ops_snapshot_metadata"))
    )

    print("Wrote dashboard-ready snapshot tables:")
    for t in [
        "waf_ops_snapshot_metadata",
        "waf_ops_snapshot_pillar_scores",
        "waf_ops_snapshot_controls",
        "waf_ops_snapshot_recommendations",
        "waf_ops_snapshot_action_plan",
    ]:
        print(f"  {WAF_CATALOG}.{WAF_SCHEMA}.{t}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Generate Walkthrough Presentation

# COMMAND ----------

pillar_rows = collect_dicts(pillar_scores_df.orderBy("completion_percent"))
controls_rows = collect_dicts(controls_df)
not_met_rows = [r for r in controls_rows if normalize_status(r.get("threshold_met")) == "Not Met"]
top_rec_rows = collect_dicts(top_recommendations_df)
action_rows = collect_dicts(action_plan_df)

total_controls = sum(safe_int(r.get("total_controls")) for r in pillar_rows)
implemented_controls = sum(safe_int(r.get("implemented_controls")) for r in pillar_rows)
overall_completion = round((implemented_controls * 100.0 / total_controls), 1) if total_controls else 0.0
not_met_count = len(not_met_rows)
critical_count = sum(1 for r in action_rows if r.get("severity") == "Critical")
high_count = sum(1 for r in action_rows if r.get("severity") == "High")

lowest_pillar = pillar_rows[0] if pillar_rows else {}

print(f"Overall completion: {overall_completion}%")
print(f"Controls: {implemented_controls}/{total_controls} met")
print(f"Not met: {not_met_count}; critical: {critical_count}; high: {high_count}")

# COMMAND ----------

def pct_bar(value):
    v = max(0, min(100, safe_float(value)))
    if v >= 80:
        cls = "good"
    elif v >= 50:
        cls = "warn"
    else:
        cls = "bad"
    return f"""
    <div class="bar"><span class="{cls}" style="width:{v:.0f}%"></span></div>
    """


def score_badge(value):
    v = safe_float(value)
    if v >= 80:
        cls = "badge good-bg"
    elif v >= 50:
        cls = "badge warn-bg"
    else:
        cls = "badge bad-bg"
    return f'<span class="{cls}">{v:.0f}%</span>'


def severity_badge(sev):
    cls = {
        "Critical": "bad-bg",
        "High": "bad-bg",
        "Medium": "warn-bg",
        "Low": "good-bg",
    }.get(sev, "neutral-bg")
    return f'<span class="badge {cls}">{escape(sev or "Unknown")}</span>'


def table_html(rows, columns, max_rows=20):
    if not rows:
        return '<p class="muted">No rows.</p>'
    cells = ["<table><thead><tr>"]
    for title, _ in columns:
        cells.append(f"<th>{escape(title)}</th>")
    cells.append("</tr></thead><tbody>")
    for row in rows[:max_rows]:
        cells.append("<tr>")
        for _, key in columns:
            val = row.get(key, "")
            if key in ("score_percentage", "threshold_percentage", "gap_to_threshold", "completion_percent"):
                val = f"{safe_float(val):.1f}"
            cells.append(f"<td>{escape(str(val) if val is not None else '')}</td>")
        cells.append("</tr>")
    cells.append("</tbody></table>")
    if len(rows) > max_rows:
        cells.append(f'<p class="muted">Showing {max_rows} of {len(rows)} rows.</p>')
    return "".join(cells)


pillar_cards = []
for r in pillar_rows:
    pillar = r.get("pillar", "Unknown")
    pct = safe_float(r.get("completion_percent"))
    total = safe_int(r.get("total_controls"))
    done = safe_int(r.get("implemented_controls"))
    pillar_cards.append(f"""
      <div class="card">
        <div class="eyebrow">{escape(pillar)}</div>
        <div class="score">{pct:.0f}%</div>
        {pct_bar(pct)}
        <div class="muted">{done} of {total} controls met</div>
      </div>
    """)


action_by_timeframe = {}
for row in action_rows:
    action_by_timeframe.setdefault(row.get("suggested_timeframe", "30-60 days"), []).append(row)


def action_list(timeframe):
    rows = action_by_timeframe.get(timeframe, [])[:8]
    if not rows:
        return '<p class="muted">No actions in this window.</p>'
    items = []
    for r in rows:
        items.append(f"""
        <li>
          <div>{severity_badge(r.get("severity"))} <b>{escape(r.get("waf_id", ""))}</b> - {escape(r.get("best_practice", ""))}</div>
          <div class="muted">{escape(r.get("suggested_owner", ""))} | Gap {safe_float(r.get("gap_to_threshold")):.1f} pts</div>
        </li>
        """)
    return "<ul>" + "".join(items) + "</ul>"


if WRITE_SNAPSHOT_TABLES:
    snapshot_table_appendix = f"""
  <h3>Dashboard-ready tables created by this notebook</h3>
  <ul>
    <li><code>{escape(WAF_CATALOG)}.{escape(WAF_SCHEMA)}.waf_ops_snapshot_metadata</code></li>
    <li><code>{escape(WAF_CATALOG)}.{escape(WAF_SCHEMA)}.waf_ops_snapshot_pillar_scores</code></li>
    <li><code>{escape(WAF_CATALOG)}.{escape(WAF_SCHEMA)}.waf_ops_snapshot_controls</code></li>
    <li><code>{escape(WAF_CATALOG)}.{escape(WAF_SCHEMA)}.waf_ops_snapshot_recommendations</code></li>
    <li><code>{escape(WAF_CATALOG)}.{escape(WAF_SCHEMA)}.waf_ops_snapshot_action_plan</code></li>
  </ul>
    """
else:
    snapshot_table_appendix = """
  <h3>Dashboard-ready tables</h3>
  <p class="muted">Snapshot table writing was disabled for this run. Set the notebook widget <code>write_snapshot_tables=true</code> to create AI/BI dashboard-ready tables.</p>
    """


run_status = escape(str(latest_run.get("status", "unknown")))
run_finished = escape(str(latest_run.get("finished_at", "")))
workspace_host = escape(str(CTX.get("workspace_host", "")))
notebook_user = escape(str(CTX.get("user", "")))

html = f"""<!doctype html>
<html>
<head>
<meta charset="utf-8">
<title>{escape(ASSESSMENT_LABEL)} - {escape(CUSTOMER_NAME)}</title>
<style>
  :root {{
    --ink:#111827; --muted:#6b7280; --line:#d1d5db; --bg:#f9fafb;
    --good:#16803c; --warn:#b45309; --bad:#b91c1c; --blue:#1d4ed8;
  }}
  * {{ box-sizing:border-box; }}
  body {{ margin:0; font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Arial,sans-serif; color:var(--ink); background:white; }}
  section {{ min-height:760px; padding:44px 52px; border-bottom:1px solid var(--line); page-break-after:always; }}
  h1 {{ font-size:38px; margin:0 0 12px; }}
  h2 {{ font-size:28px; margin:0 0 18px; }}
  h3 {{ font-size:18px; margin:24px 0 10px; }}
  p {{ line-height:1.45; }}
  .muted {{ color:var(--muted); font-size:13px; }}
  .eyebrow {{ color:var(--muted); text-transform:uppercase; letter-spacing:.04em; font-size:12px; font-weight:700; }}
  .grid {{ display:grid; grid-template-columns:repeat(4,1fr); gap:14px; }}
  .grid3 {{ display:grid; grid-template-columns:repeat(3,1fr); gap:14px; }}
  .card {{ border:1px solid var(--line); border-radius:8px; padding:16px; background:#fff; }}
  .hero {{ display:grid; grid-template-columns:1.1fr .9fr; gap:28px; align-items:center; }}
  .big-number {{ font-size:76px; font-weight:800; line-height:1; }}
  .score {{ font-size:34px; font-weight:800; margin:10px 0 8px; }}
  .bar {{ height:9px; background:#e5e7eb; border-radius:99px; overflow:hidden; margin:8px 0; }}
  .bar span {{ display:block; height:100%; }}
  .good {{ background:var(--good); }} .warn {{ background:var(--warn); }} .bad {{ background:var(--bad); }}
  .badge {{ display:inline-block; border-radius:999px; padding:4px 8px; color:#fff; font-size:12px; font-weight:700; }}
  .good-bg {{ background:var(--good); }} .warn-bg {{ background:var(--warn); }} .bad-bg {{ background:var(--bad); }} .neutral-bg {{ background:var(--muted); }}
  table {{ width:100%; border-collapse:collapse; font-size:12px; margin-top:8px; }}
  th {{ text-align:left; background:#f3f4f6; border-bottom:1px solid var(--line); padding:8px; }}
  td {{ border-bottom:1px solid #e5e7eb; padding:8px; vertical-align:top; }}
  ul {{ padding-left:18px; }}
  li {{ margin:0 0 10px; }}
  .callout {{ border-left:4px solid var(--blue); background:#eff6ff; padding:12px 14px; margin:18px 0; }}
  .footer {{ position:fixed; bottom:12px; left:52px; right:52px; color:#9ca3af; font-size:11px; }}
  @media print {{
    section {{ min-height:7.5in; }}
    .footer {{ position:static; margin-top:20px; }}
  }}
</style>
</head>
<body>

<section>
  <div class="hero">
    <div>
      <div class="eyebrow">Databricks Well-Architected Framework</div>
      <h1>{escape(ASSESSMENT_LABEL)}</h1>
      <p>{escape(CUSTOMER_NAME)}</p>
      <p class="muted">Generated {SNAPSHOT_TS.strftime('%Y-%m-%d %H:%M:%S')} UTC by {notebook_user}</p>
      <p class="muted">Workspace: {workspace_host}</p>
      <div class="callout">
        This pack is generated from the customer's WAF Light Tooling cache tables in
        <b>{escape(WAF_CATALOG)}.{escape(WAF_SCHEMA)}</b>. It is intended as a walkthrough snapshot for ops,
        platform, governance, FinOps, and workload owners.
      </div>
    </div>
    <div class="card">
      <div class="eyebrow">Overall WAF Completion</div>
      <div class="big-number">{overall_completion:.0f}%</div>
      {pct_bar(overall_completion)}
      <p>{implemented_controls} of {total_controls} controls met</p>
      <p class="muted">Latest reload run: {run_status} {run_finished}</p>
    </div>
  </div>
</section>

<section>
  <h2>Executive Scorecard</h2>
  <div class="grid">
    {''.join(pillar_cards)}
  </div>
  <h3>Interpretation</h3>
  <ul>
    <li><b>{not_met_count}</b> controls are currently not meeting threshold.</li>
    <li><b>{critical_count}</b> critical and <b>{high_count}</b> high-priority actions should be reviewed first.</li>
    <li>Lowest pillar: <b>{escape(str(lowest_pillar.get('pillar', 'n/a')))}</b> at {safe_float(lowest_pillar.get('completion_percent')):.0f}%.</li>
  </ul>
</section>

<section>
  <h2>Top Recommendations</h2>
  <p class="muted">Prioritized by severity and score gap to threshold.</p>
  {table_html(top_rec_rows, [
      ("Severity", "severity"),
      ("WAF ID", "waf_id"),
      ("Pillar", "pillar"),
      ("Score", "score_percentage"),
      ("Threshold", "threshold_percentage"),
      ("Gap", "gap_to_threshold"),
      ("Owner", "suggested_owner"),
      ("Recommendation", "recommendation"),
  ], TOP_N)}
</section>

<section>
  <h2>0-30 Day Action Plan</h2>
  <div class="grid3">
    <div class="card">
      <h3>0-30 days</h3>
      {action_list("0-30 days")}
    </div>
    <div class="card">
      <h3>30-60 days</h3>
      {action_list("30-60 days")}
    </div>
    <div class="card">
      <h3>60-90 days</h3>
      {action_list("60-90 days")}
    </div>
  </div>
</section>

<section>
  <h2>Pillar Breakdown</h2>
  {table_html(pillar_rows, [
      ("Pillar", "pillar"),
      ("Total Controls", "total_controls"),
      ("Controls Met", "implemented_controls"),
      ("Completion %", "completion_percent"),
  ], 20)}
  <h3>Suggested talking points</h3>
  <ul>
    <li>Confirm whether the latest WAF reload run succeeded or was partial.</li>
    <li>Review the lowest scoring pillar first, then the critical/high recommendations.</li>
    <li>Agree owners and dates for the first 30-day remediation window.</li>
    <li>Schedule a follow-up reload after remediation to measure score movement.</li>
  </ul>
</section>

<section>
  <h2>All Not-Met Controls</h2>
  {table_html(action_rows, [
      ("Timeframe", "suggested_timeframe"),
      ("Severity", "severity"),
      ("WAF ID", "waf_id"),
      ("Pillar", "pillar"),
      ("Best Practice", "best_practice"),
      ("Score", "score_percentage"),
      ("Threshold", "threshold_percentage"),
      ("Gap", "gap_to_threshold"),
      ("Owner", "suggested_owner"),
  ], 60)}
</section>

<section>
  <h2>Appendix: Data Sources and Caveats</h2>
  <ul>
    <li>Source tables: <b>{escape(WAF_CATALOG)}.{escape(WAF_SCHEMA)}</b>.</li>
    <li>Primary WAF outputs used: <code>waf_total_percentage_across_pillars</code>, <code>waf_controls_g</code>, <code>waf_controls_c</code>, <code>waf_controls_p</code>, <code>waf_controls_r</code>, and <code>waf_recommendations_not_met</code>.</li>
    <li>Scores reflect the latest WAF reload job, not real-time state.</li>
    <li>Missing system table permissions or greenfield workspaces can produce partial results. Check <code>_run_log</code> for reload status.</li>
    <li>This report is a decision-support snapshot. Validate recommended actions with workload owners before making production changes.</li>
  </ul>
  {snapshot_table_appendix}
</section>

</body>
</html>
"""

displayHTML(html)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Save Report Artifact

# COMMAND ----------

dbutils.fs.mkdirs(OUTPUT_DIR)
html_path = f"{OUTPUT_DIR}/waf_ops_snapshot_{SNAPSHOT_ID}.html"
dbutils.fs.put(html_path, html, overwrite=True)

print(f"Saved HTML presentation: {html_path}")

if OUTPUT_DIR.startswith("dbfs:/FileStore/") and CTX.get("workspace_host"):
    rel = html_path.replace("dbfs:/FileStore/", "")
    print(f"Browser URL: https://{CTX['workspace_host']}/files/{rel}")

# Also write the action plan as CSV for spreadsheet follow-up.
csv_dir = f"{OUTPUT_DIR}/waf_ops_snapshot_{SNAPSHOT_ID}_action_plan_csv"
(
    action_plan_df
    .coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(csv_dir)
)
print(f"Saved action-plan CSV folder: {csv_dir}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Optional AI/BI Dashboard Queries
# MAGIC
# MAGIC If you want a lightweight AI/BI dashboard instead of the generated HTML deck, create visualizations from these snapshot tables:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT * FROM <catalog>.waf_cache.waf_ops_snapshot_metadata;
# MAGIC SELECT * FROM <catalog>.waf_cache.waf_ops_snapshot_pillar_scores ORDER BY completion_percent;
# MAGIC SELECT * FROM <catalog>.waf_cache.waf_ops_snapshot_action_plan ORDER BY suggested_timeframe, severity, gap_to_threshold DESC;
# MAGIC SELECT pillar, severity, count(*) AS action_count
# MAGIC FROM <catalog>.waf_cache.waf_ops_snapshot_action_plan
# MAGIC GROUP BY pillar, severity;
# MAGIC ```
