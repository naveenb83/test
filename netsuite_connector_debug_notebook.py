# Databricks notebook source
# MAGIC %md
# MAGIC # NetSuite Connector Diagnostic + Sequential Query Runner
# MAGIC
# MAGIC This notebook is designed for isolating problematic NetSuite queries while staying close to Lakeflow connector behavior.
# MAGIC
# MAGIC ## Customer Runbook (single checklist)
# MAGIC
# MAGIC 1. **Use the same connector as the failing pipeline**
# MAGIC    - In pipeline YAML, copy `ingestion_definition.connection_name`
# MAGIC    - Set notebook widget `connection_name` to that exact value
# MAGIC    - Do not create a new connection unless intentionally testing different credentials
# MAGIC
# MAGIC 2. **Use personal compute + attach JAR**
# MAGIC    - Compute must be **Dedicated** (single-user / personal compute)
# MAGIC    - Install JAR from `netsuite_jar_path` shown in pipeline YAML
# MAGIC
# MAGIC 3. **Grant minimum required permissions to the user who runs this notebook**
# MAGIC    - From `netsuite_jar_path = /Volumes/<catalog>/<schema>/<volume>/<jar>.jar`, grant:
# MAGIC
# MAGIC ```sql
# MAGIC GRANT USE_CATALOG ON CATALOG <catalog> TO `<user>`;
# MAGIC GRANT USE_SCHEMA ON SCHEMA <catalog>.<schema> TO `<user>`;
# MAGIC GRANT READ VOLUME ON VOLUME <catalog>.<schema>.<volume> TO `<user>`;
# MAGIC ```
# MAGIC
# MAGIC    - On the connection object:
# MAGIC
# MAGIC ```sql
# MAGIC GRANT USE CONNECTION ON CONNECTION <connection_name> TO `<user>`;
# MAGIC GRANT MANAGE CONNECTION ON CONNECTION <connection_name> TO `<user>`;
# MAGIC ```
# MAGIC
# MAGIC 4. **Set widgets and run order**
# MAGIC    - `mode = jdbc` (recommended)
# MAGIC    - `connection_name = <pipeline connection_name>`
# MAGIC    - `jar_path = <full netsuite_jar_path>`
# MAGIC    - `apply_keepalive_reflection = true` (recommended for long/idle metadata calls)
# MAGIC    - `queries_json = ["SELECT ...", "SELECT ..."]`
# MAGIC    - Run notebook top-to-bottom
# MAGIC
# MAGIC 5. **If a query fails, share back**
# MAGIC    - Failing query text
# MAGIC    - `query_index`, `status`, `elapsed_ms`, `error_message` from summary
# MAGIC    - Full SQLException chain printed in cell output
# MAGIC
# MAGIC It provides two execution modes:
# MAGIC 1. **JDBC mode (recommended)**: Uses the NetSuite OpenAccess JDBC driver directly.
# MAGIC 2. **Spark connector mode**: Uses `spark.read.format(...)` for quick query checks.
# MAGIC
# MAGIC Run cells top-to-bottom.
# MAGIC
# MAGIC ## Preflight (must do first)
# MAGIC
# MAGIC - Use a **Personal Compute** cluster with access mode **Dedicated** (single-user).
# MAGIC - Do **not** use serverless compute for NetSuite JAR diagnostics.
# MAGIC - Install the customer NetSuite OpenAccess JAR from the volume path in `pipeline_spec.netsuite_jar_path`.
# MAGIC - Ensure support principal has volume access:
# MAGIC
# MAGIC ```sql
# MAGIC GRANT USE_CATALOG ON CATALOG <catalog> TO `DB - RESERVED - Databricks support`;
# MAGIC GRANT USE_SCHEMA ON SCHEMA <catalog>.<schema> TO `DB - RESERVED - Databricks support`;
# MAGIC GRANT READ VOLUME ON VOLUME <catalog>.<schema>.<volume> TO `DB - RESERVED - Databricks support`;
# MAGIC ```
# MAGIC
# MAGIC Reference notebook:
# MAGIC [NetSuite Connector Diagnostic Notebook](https://github.com/databricks-eng/ingestion-notebooks/blob/main/saas-connectors/NetSuite%20Connector%20Diagnostic%20Notebook.ipynb)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1) Common Widgets
# MAGIC
# MAGIC Set these before running helper and execution cells.

# COMMAND ----------

dbutils.widgets.text("mode", "jdbc", "Mode: jdbc or spark")
dbutils.widgets.text("query_timeout_seconds", "900", "Per-query timeout (seconds)")
dbutils.widgets.text("sample_rows", "20", "Rows to display")
dbutils.widgets.text("queries_json", """[
  "SELECT * FROM vendor",
  "SELECT COUNT(*) AS cnt FROM transaction"
]""", "Queries JSON array")

# Spark mode widgets
dbutils.widgets.text("connector_format", "netsuite", "Spark connector format")
dbutils.widgets.text("query_option_key", "query", "Spark query option key")
dbutils.widgets.text("connector_options_json", """{
  "account": "<account_id>",
  "role": "<role_id>",
  "user": "<username>",
  "password": "<password>"
}""", "Spark connector options JSON")

# JDBC mode widgets
dbutils.widgets.text("connection_name", "", "UC Connection Name")
dbutils.widgets.text("jar_path", "", "JAR Path (Volume)")
dbutils.widgets.text("apply_keepalive_reflection", "false", "Apply keepalive reflection (true/false)")

print("Widgets initialized.")

# COMMAND ----------
# MAGIC %scala
# MAGIC // Helper functions for JDBC mode (connection + sequential execution).
# MAGIC // This intentionally mirrors the diagnostics path more closely than spark.read.format(...).
# MAGIC
# MAGIC import java.net.{URI}
# MAGIC import java.net.http.{HttpClient, HttpRequest, HttpResponse}
# MAGIC import java.net.http.HttpRequest.BodyPublishers
# MAGIC import java.io.{File, FileInputStream}
# MAGIC import java.net.Socket
# MAGIC import java.security.{MessageDigest, SecureRandom}
# MAGIC import java.sql.{Connection, DriverManager, SQLException}
# MAGIC import java.time.Instant
# MAGIC import java.util.{Base64, Properties}
# MAGIC import javax.crypto.Mac
# MAGIC import javax.crypto.spec.SecretKeySpec
# MAGIC import scala.jdk.CollectionConverters._
# MAGIC import scala.util.control.NonFatal
# MAGIC import com.fasterxml.jackson.databind.ObjectMapper
# MAGIC
# MAGIC val CONNECTION_NAME: String = dbutils.widgets.get("connection_name").trim
# MAGIC val JAR_PATH: String = dbutils.widgets.get("jar_path").trim
# MAGIC val APPLY_KEEPALIVE: Boolean =
# MAGIC   dbutils.widgets.get("apply_keepalive_reflection").trim.toLowerCase == "true"
# MAGIC val DRIVER_CLASS = "com.netsuite.jdbc.openaccess.OpenAccessDriver"
# MAGIC val SERVER_DATA_SOURCE = "NetSuite2.com"
# MAGIC
# MAGIC val ctx = dbutils.notebook.getContext()
# MAGIC val WORKSPACE_URL: String = ctx.apiUrl.get
# MAGIC val API_TOKEN: String = ctx.apiToken.get
# MAGIC val mapper = new ObjectMapper()
# MAGIC
# MAGIC def getConnectionCredentials(name: String): Map[String, String] = {
# MAGIC   require(name.nonEmpty, "connection_name widget is empty")
# MAGIC   val client = HttpClient.newHttpClient()
# MAGIC   val url = s"$WORKSPACE_URL/api/2.1/unity-catalog/foreign-credentials/"
# MAGIC   val body = s"""{"securables":[{"type":"CONNECTION","full_name":"$name"}]}"""
# MAGIC   val request = HttpRequest.newBuilder()
# MAGIC     .uri(URI.create(url))
# MAGIC     .header("Authorization", s"Bearer $API_TOKEN")
# MAGIC     .header("Content-Type", "application/json")
# MAGIC     .POST(BodyPublishers.ofString(body))
# MAGIC     .build()
# MAGIC
# MAGIC   val response = client.send(request, HttpResponse.BodyHandlers.ofString())
# MAGIC   if (response.statusCode() != 200) {
# MAGIC     throw new RuntimeException(s"foreign-credentials failed: ${response.statusCode()} ${response.body()}")
# MAGIC   }
# MAGIC
# MAGIC   val node = mapper.readTree(response.body())
# MAGIC     .path("securable_to_credentials").path(0)
# MAGIC     .path("credentials").path("foreign_credential")
# MAGIC     .path("options").path("options")
# MAGIC
# MAGIC   if (node.isMissingNode || node.isNull) {
# MAGIC     throw new RuntimeException("No credentials found. Ensure USE_CONNECTION and MANAGE_CONNECTION.")
# MAGIC   }
# MAGIC
# MAGIC   node.fields().asScala.map(e => e.getKey -> e.getValue.asText()).toMap
# MAGIC }
# MAGIC
# MAGIC def generateNonce(): String = {
# MAGIC   val bytes = new Array[Byte](12)
# MAGIC   new SecureRandom().nextBytes(bytes)
# MAGIC   Base64.getEncoder.encodeToString(bytes)
# MAGIC }
# MAGIC
# MAGIC def hmacSha256(data: String, key: String): Array[Byte] = {
# MAGIC   val mac = Mac.getInstance("HmacSHA256")
# MAGIC   mac.init(new SecretKeySpec(key.getBytes("UTF-8"), "HmacSHA256"))
# MAGIC   mac.doFinal(data.getBytes("UTF-8"))
# MAGIC }
# MAGIC
# MAGIC def generateTokenPassword(
# MAGIC     accountId: String,
# MAGIC     consumerKey: String,
# MAGIC     consumerSecret: String,
# MAGIC     tokenId: String,
# MAGIC     tokenSecret: String): String = {
# MAGIC   val nonce = generateNonce()
# MAGIC   val timestamp = Instant.now().getEpochSecond.toString
# MAGIC   val baseString = s"$accountId&$consumerKey&$tokenId&$nonce&$timestamp"
# MAGIC   val signatureKey = s"$consumerSecret&$tokenSecret"
# MAGIC   val signature =
# MAGIC     Base64.getEncoder.encodeToString(hmacSha256(baseString, signatureKey)) + "&HMAC-SHA256"
# MAGIC   s"$baseString&$signature"
# MAGIC }
# MAGIC
# MAGIC def printSqlExceptionChain(t: Throwable): Unit = {
# MAGIC   println("--- SQLException chain ---")
# MAGIC   var cur: Throwable = t
# MAGIC   while (cur != null) {
# MAGIC     cur match {
# MAGIC       case se: SQLException =>
# MAGIC         println(s"class=${se.getClass.getName} sqlState=${se.getSQLState} vendorCode=${se.getErrorCode}")
# MAGIC         println(s"message=${se.getMessage}")
# MAGIC         var next = se.getNextException
# MAGIC         while (next != null) {
# MAGIC           println(s"  next.sqlState=${next.getSQLState} vendorCode=${next.getErrorCode} message=${next.getMessage}")
# MAGIC           next = next.getNextException
# MAGIC         }
# MAGIC       case other =>
# MAGIC         println(s"class=${other.getClass.getName} message=${other.getMessage}")
# MAGIC     }
# MAGIC     cur = cur.getCause
# MAGIC   }
# MAGIC   println("--------------------------")
# MAGIC }
# MAGIC
# MAGIC // Keepalive reflection mapping by JAR hash.
# MAGIC // Supported builds:
# MAGIC // - 8.10.184.0
# MAGIC // - 8.1.00.0170
# MAGIC val NETSUITE_8_10_184_0_PATH: Seq[String] =
# MAGIC   Seq("implConnection", "LV", "RF", "QZ", "adW", "acZ", "adD", "adK")
# MAGIC val NETSUITE_8_1_00_0170_PATH: Seq[String] =
# MAGIC   Seq("implConnection", "Jy", "Ol", "aaQ", "ZT", "aax", "aaE")
# MAGIC
# MAGIC val FieldPathByJarHash: Map[String, Seq[String]] = Map(
# MAGIC   "187d918a76dd52335baeddf6a9724d26f94e1ceb48c9cf0edcc4fe3eb1d2afab" -> NETSUITE_8_10_184_0_PATH,
# MAGIC   "4ac243c003e8aff176f72f59168887e76ddac63f5cc164e4a495e0c597358af4" -> NETSUITE_8_1_00_0170_PATH
# MAGIC )
# MAGIC
# MAGIC def sha256Hex(path: String): String = {
# MAGIC   val md = MessageDigest.getInstance("SHA-256")
# MAGIC   val in = new FileInputStream(new File(path))
# MAGIC   try {
# MAGIC     val buf = new Array[Byte](64 * 1024)
# MAGIC     var n = in.read(buf)
# MAGIC     while (n > 0) { md.update(buf, 0, n); n = in.read(buf) }
# MAGIC   } finally in.close()
# MAGIC   md.digest().map(b => f"$b%02x").mkString
# MAGIC }
# MAGIC
# MAGIC def readField(obj: AnyRef, name: String): AnyRef = {
# MAGIC   var cls: Class[_] = obj.getClass
# MAGIC   while (cls != null) {
# MAGIC     try {
# MAGIC       val f = cls.getDeclaredField(name)
# MAGIC       f.setAccessible(true)
# MAGIC       return f.get(obj)
# MAGIC     } catch { case _: NoSuchFieldException => cls = cls.getSuperclass }
# MAGIC   }
# MAGIC   throw new NoSuchFieldException(name)
# MAGIC }
# MAGIC
# MAGIC def walkPath(root: AnyRef, path: Seq[String]): AnyRef =
# MAGIC   path.foldLeft[AnyRef](root)(readField)
# MAGIC
# MAGIC def applyKeepAliveReflection(conn: Connection, jarHash: String): Boolean = {
# MAGIC   FieldPathByJarHash.get(jarHash) match {
# MAGIC     case Some(path) =>
# MAGIC       try {
# MAGIC         walkPath(conn, path).asInstanceOf[Socket].setKeepAlive(true)
# MAGIC         println(s"[keepalive] applied SO_KEEPALIVE via field path for jar hash $jarHash")
# MAGIC         true
# MAGIC       } catch {
# MAGIC         case NonFatal(e) =>
# MAGIC           println(s"[keepalive] FAILED for hash $jarHash: ${e.getMessage}")
# MAGIC           false
# MAGIC       }
# MAGIC     case None =>
# MAGIC       println(
# MAGIC         s"[keepalive] no field-path mapping for jar hash $jarHash; skipping. " +
# MAGIC         "Known: 8.10.184.0, 8.1.00.0170.")
# MAGIC       false
# MAGIC   }
# MAGIC }
# MAGIC
# MAGIC def connectNetSuite(): Connection = {
# MAGIC   val opts = getConnectionCredentials(CONNECTION_NAME)
# MAGIC   val required = Seq(
# MAGIC     "host", "port", "account_id", "role_id",
# MAGIC     "consumer_key", "consumer_secret", "token_id", "token_secret")
# MAGIC   val missing = required.filterNot(opts.contains)
# MAGIC   if (missing.nonEmpty) {
# MAGIC     throw new RuntimeException(s"Missing options in UC connection: ${missing.mkString(", ")}")
# MAGIC   }
# MAGIC
# MAGIC   val password = generateTokenPassword(
# MAGIC     opts("account_id"), opts("consumer_key"), opts("consumer_secret"),
# MAGIC     opts("token_id"), opts("token_secret"))
# MAGIC
# MAGIC   Class.forName(DRIVER_CLASS)
# MAGIC   val url = s"jdbc:ns://${opts("host")}:${opts("port")};encrypted=1;uppercase=1;NegotiateSSLClose=false"
# MAGIC   val props = new Properties()
# MAGIC   props.setProperty("user", "TBA")
# MAGIC   props.setProperty("password", password)
# MAGIC   props.setProperty("ServerDataSource", SERVER_DATA_SOURCE)
# MAGIC   props.setProperty("CustomProperties", s"(AccountID=${opts("account_id")};RoleID=${opts("role_id")})")
# MAGIC
# MAGIC   val conn = DriverManager.getConnection(url, props)
# MAGIC   if (APPLY_KEEPALIVE) {
# MAGIC     if (JAR_PATH.isEmpty) {
# MAGIC       println("[keepalive] apply_keepalive_reflection=true but jar_path is empty; skipping.")
# MAGIC     } else {
# MAGIC       try {
# MAGIC         val hash = sha256Hex(JAR_PATH)
# MAGIC         println(s"[keepalive] JAR sha256 = $hash")
# MAGIC         applyKeepAliveReflection(conn, hash)
# MAGIC       } catch {
# MAGIC         case NonFatal(e) =>
# MAGIC           println(s"[keepalive] could not apply keepalive: ${e.getMessage}")
# MAGIC       }
# MAGIC     }
# MAGIC   } else {
# MAGIC     println("[keepalive] disabled by widget")
# MAGIC   }
# MAGIC   conn
# MAGIC }
# MAGIC
# MAGIC def validateConnection(): Unit = {
# MAGIC   try {
# MAGIC     val conn = connectNetSuite()
# MAGIC     try {
# MAGIC       val md = conn.getMetaData
# MAGIC       println(s"[ok] driver=${md.getDriverName} ${md.getDriverVersion}")
# MAGIC       println(s"     database=${md.getDatabaseProductName} ${md.getDatabaseProductVersion}")
# MAGIC       println(s"     url=${md.getURL}")
# MAGIC     } finally conn.close()
# MAGIC   } catch {
# MAGIC     case NonFatal(e) =>
# MAGIC       println("[fail] connection validation failed")
# MAGIC       printSqlExceptionChain(e)
# MAGIC   }
# MAGIC }
# MAGIC
# MAGIC case class QueryResult(query_index: Int, status: String, elapsed_ms: Long, row_count: Long, error_message: String, query: String)
# MAGIC
# MAGIC def runQueriesSequentially(queries: Seq[String], sampleRows: Int): Seq[QueryResult] = {
# MAGIC   queries.zipWithIndex.map { case (q, idx0) =>
# MAGIC     val idx = idx0 + 1
# MAGIC     val started = System.currentTimeMillis()
# MAGIC     var rowCount = -1L
# MAGIC     var status = "SUCCESS"
# MAGIC     var err: String = null
# MAGIC
# MAGIC     println("=" * 100)
# MAGIC     println(s"Running query $idx/${queries.size}")
# MAGIC     println(q)
# MAGIC
# MAGIC     try {
# MAGIC       val conn = connectNetSuite()
# MAGIC       try {
# MAGIC         val stmt = conn.createStatement()
# MAGIC         try {
# MAGIC           stmt.setMaxRows(sampleRows)
# MAGIC           val rs = stmt.executeQuery(q)
# MAGIC           val md = rs.getMetaData
# MAGIC           val colCount = md.getColumnCount
# MAGIC           var printed = 0
# MAGIC           while (rs.next() && printed < sampleRows) {
# MAGIC             val cols = (1 to colCount).map(i => s"${md.getColumnName(i)}=${Option(rs.getObject(i)).getOrElse("null")}")
# MAGIC             println(cols.mkString(" | "))
# MAGIC             printed += 1
# MAGIC           }
# MAGIC           rowCount = printed.toLong
# MAGIC         } finally stmt.close()
# MAGIC       } finally conn.close()
# MAGIC     } catch {
# MAGIC       case NonFatal(e) =>
# MAGIC         status = "FAILED"
# MAGIC         err = s"${e.getClass.getSimpleName}: ${e.getMessage}"
# MAGIC         printSqlExceptionChain(e)
# MAGIC     }
# MAGIC
# MAGIC     val elapsed = System.currentTimeMillis() - started
# MAGIC     QueryResult(idx, status, elapsed, rowCount, err, q)
# MAGIC   }
# MAGIC }
# MAGIC
# MAGIC println("JDBC helper functions loaded.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2) Optional: validate JDBC connectivity first

# COMMAND ----------
# MAGIC %scala
# MAGIC // Uncomment to run a quick connectivity check.
# MAGIC // validateConnection()

# COMMAND ----------

from datetime import datetime
import json
import traceback

mode = dbutils.widgets.get("mode").strip().lower()
sample_rows = int(dbutils.widgets.get("sample_rows").strip())
queries = json.loads(dbutils.widgets.get("queries_json"))

if not isinstance(queries, list) or not queries:
    raise ValueError("queries_json must be a non-empty JSON array of query strings.")

print(f"Loaded {len(queries)} query(ies) in mode={mode}.")

# COMMAND ----------

if mode == "spark":
    connector_format = dbutils.widgets.get("connector_format").strip()
    query_option_key = dbutils.widgets.get("query_option_key").strip()
    query_timeout_seconds = int(dbutils.widgets.get("query_timeout_seconds").strip())
    connector_options = json.loads(dbutils.widgets.get("connector_options_json"))

    spark.conf.set("spark.sql.execution.timeout", f"{query_timeout_seconds}s")
    results = []

    for idx, query in enumerate(queries, start=1):
        started_at = datetime.utcnow()
        print("=" * 100)
        print(f"Running query {idx}/{len(queries)}")
        print(query)

        status = "SUCCESS"
        error_message = None
        row_count = None

        try:
            reader = spark.read.format(connector_format)
            for k, v in connector_options.items():
                reader = reader.option(k, str(v))
            df = reader.option(query_option_key, query).load()
            row_count = df.count()
            print(f"Row count: {row_count}")
            display(df.limit(sample_rows))
        except Exception as exc:
            status = "FAILED"
            error_message = f"{type(exc).__name__}: {exc}"
            print("Query failed:")
            print(error_message)
            print(traceback.format_exc())

        finished_at = datetime.utcnow()
        elapsed_seconds = round((finished_at - started_at).total_seconds(), 2)
        results.append(
            {
                "query_index": idx,
                "status": status,
                "elapsed_seconds": elapsed_seconds,
                "row_count": row_count,
                "error_message": error_message,
                "query": query,
                "started_at_utc": started_at.isoformat(),
                "finished_at_utc": finished_at.isoformat(),
            }
        )

    summary_df = spark.createDataFrame(results)
    display(summary_df.orderBy("query_index"))
else:
    print("Skipping Spark mode cell execution because mode != spark.")

# COMMAND ----------
# MAGIC %scala
# MAGIC // JDBC mode sequential execution and summary table.
# MAGIC // Use this mode when you need behavior closest to the NetSuite JDBC path.
# MAGIC
# MAGIC val MODE = dbutils.widgets.get("mode").trim.toLowerCase
# MAGIC val SAMPLE_ROWS = dbutils.widgets.get("sample_rows").trim.toInt
# MAGIC val QUERIES_JSON = dbutils.widgets.get("queries_json").trim
# MAGIC
# MAGIC if (MODE == "jdbc") {
# MAGIC   // Parse JSON array of strings with Jackson to avoid schema assumptions.
# MAGIC   val mapper = new com.fasterxml.jackson.databind.ObjectMapper()
# MAGIC   val node = mapper.readTree(QUERIES_JSON)
# MAGIC   require(node.isArray && node.size() > 0, "queries_json must be a non-empty JSON array")
# MAGIC   val queries = (0 until node.size()).map(i => node.get(i).asText())
# MAGIC
# MAGIC   val results = runQueriesSequentially(queries, SAMPLE_ROWS)
# MAGIC   import spark.implicits._
# MAGIC   display(results.toDF.orderBy("query_index"))
# MAGIC } else {
# MAGIC   println("Skipping JDBC mode cell execution because mode != jdbc.")
# MAGIC }
