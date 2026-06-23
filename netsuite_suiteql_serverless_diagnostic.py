# Databricks notebook source
# MAGIC %md
# MAGIC # NetSuite SuiteQL REST Diagnostic Notebook (Serverless)
# MAGIC
# MAGIC Tests NetSuite connectivity using the REST/SuiteQL API — no JAR required, runs on serverless compute.
# MAGIC
# MAGIC ## How to use
# MAGIC 1. Set `connection_name` to the UC connection used by the failing pipeline
# MAGIC 2. Set a test query in `query` (SuiteQL syntax)
# MAGIC 3. Run cells top to bottom
# MAGIC
# MAGIC ## Grants required
# MAGIC The user running this notebook must have:
# MAGIC ```sql
# MAGIC GRANT USE CONNECTION ON CONNECTION <connection_name> TO `<user>`;
# MAGIC GRANT MANAGE CONNECTION ON CONNECTION <connection_name> TO `<user>`;
# MAGIC ```

# COMMAND ----------

%pip install requests-oauthlib -q

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("connection_name", "", "UC Connection Name")
dbutils.widgets.text("query", "SELECT id, companyName FROM customer LIMIT 5", "SuiteQL Query")
dbutils.widgets.text("sample_rows", "20", "Rows to display")
dbutils.widgets.text("account_id_override", "", "Account ID override (leave blank to auto-detect from connection)")
dbutils.widgets.text("timeout_seconds", "60", "Request timeout (seconds)")

print("Widgets initialised.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1) Fetch credentials from UC connection

# COMMAND ----------

import json
import urllib.request

CONNECTION_NAME = dbutils.widgets.get("connection_name").strip()
ACCOUNT_ID_OVERRIDE = dbutils.widgets.get("account_id_override").strip()
SAMPLE_ROWS = int(dbutils.widgets.get("sample_rows").strip())
TIMEOUT = int(dbutils.widgets.get("timeout_seconds").strip())

assert CONNECTION_NAME, "connection_name widget is empty — set it before running"

ctx = dbutils.notebook.getContext()
workspace_url = ctx.apiUrl().get()
api_token = ctx.apiToken().get()

body = json.dumps({
    "securables": [{"type": "CONNECTION", "full_name": CONNECTION_NAME}]
}).encode()

req = urllib.request.Request(
    f"{workspace_url}/api/2.1/unity-catalog/foreign-credentials/",
    data=body,
    headers={
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json",
    },
    method="POST",
)

try:
    with urllib.request.urlopen(req) as resp:
        data = json.loads(resp.read())
except urllib.error.HTTPError as e:
    raise RuntimeError(
        f"Failed to fetch credentials (HTTP {e.code}): {e.read().decode()}. "
        "Ensure USE CONNECTION and MANAGE CONNECTION are granted."
    )

creds = (
    data["securable_to_credentials"][0]
    ["credentials"]["foreign_credential"]["options"]["options"]
)

# NetSuite REST API uses hyphens and lowercase in the subdomain, but the original
# account_id (with underscores/uppercase) is used as the OAuth realm.
ACCOUNT_ID_RAW = creds["account_id"]
ACCOUNT_ID_URL = ACCOUNT_ID_OVERRIDE if ACCOUNT_ID_OVERRIDE else ACCOUNT_ID_RAW.replace("_", "-").lower()

print(f"[ok] credentials fetched")
print(f"     connection  = {CONNECTION_NAME}")
print(f"     account_id  = {ACCOUNT_ID_RAW}")
print(f"     url account = {ACCOUNT_ID_URL}")
print(f"     host        = {creds.get('host', 'n/a')}")
print(f"     role_id     = {creds.get('role_id', 'n/a')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2) Validate connectivity (HEAD request to REST endpoint)

# COMMAND ----------

import requests
from requests_oauthlib import OAuth1

def make_auth():
    return OAuth1(
        client_key=creds["consumer_key"],
        client_secret=creds["consumer_secret"],
        resource_owner_key=creds["token_id"],
        resource_owner_secret=creds["token_secret"],
        signature_method="HMAC-SHA256",
        realm=ACCOUNT_ID_RAW,
    )

SUITEQL_URL = f"https://{ACCOUNT_ID_URL}.suitetalk.api.netsuite.com/services/rest/query/v1/suiteql"

print(f"Testing connectivity to:\n  {SUITEQL_URL}\n")

try:
    probe = requests.options(SUITEQL_URL, auth=make_auth(), timeout=TIMEOUT)
    print(f"[ok] endpoint reachable — HTTP {probe.status_code}")
except requests.exceptions.ConnectionError as e:
    print(f"[fail] cannot reach endpoint — ConnectionError: {e}")
    print("       Check: does this cluster have internet access? Is the account_id correct?")
    raise
except requests.exceptions.Timeout:
    print(f"[fail] timed out after {TIMEOUT}s — cluster likely has no internet egress")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3) Run SuiteQL query

# COMMAND ----------

QUERY = dbutils.widgets.get("query").strip()
assert QUERY, "query widget is empty — enter a SuiteQL query before running"

print(f"Query:\n{QUERY}\n")

response = requests.post(
    SUITEQL_URL,
    auth=make_auth(),
    headers={"prefer": "transient"},
    json={"q": QUERY},
    timeout=TIMEOUT,
)

if response.status_code != 200:
    print(f"[fail] HTTP {response.status_code}")
    try:
        err = response.json()
        print(json.dumps(err, indent=2))
    except Exception:
        print(response.text)
else:
    result = response.json()
    items = result.get("items", [])
    total = result.get("totalResults", len(items))
    has_more = result.get("hasMore", False)

    print(f"[ok] HTTP 200")
    print(f"     totalResults = {total}")
    print(f"     hasMore      = {has_more}")
    print(f"     showing up to {SAMPLE_ROWS} row(s)\n")

    if items:
        display(spark.createDataFrame(items[:SAMPLE_ROWS]))
    else:
        print("No rows returned.")
