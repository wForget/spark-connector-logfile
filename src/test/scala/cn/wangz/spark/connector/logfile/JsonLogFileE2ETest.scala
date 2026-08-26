package cn.wangz.spark.connector.logfile

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

class JsonLogFileE2ETest extends LogFileTestBase {

  test("read json log files via catalog") {
    val logDir = resourcePath("json_logs")
    withCatalog("json_cat", logDir, "json") {
      val df = spark.sql("SELECT * FROM json_cat.default.spark_log_file")

      assert(df.columns.toSet === Set("value", "dt", "hour", "app_id"))
      assert(df.count() === 4)

      val values = df.filter("app_id = 'app_json_001'")
        .select("value").collect().map(_.getString(0)).toSet
      assert(values === Set("json event 1", "json event 2", "json event 3"))

      val app2Values = df.filter("app_id = 'app_json_002'")
        .select("value").collect().map(_.getString(0)).toSet
      assert(app2Values === Set("json event 4"))
    }
  }

  test("read json log files with extra fields ignored") {
    val logDir = resourcePath("json_extra_logs")
    withCatalog("json_extra_cat", logDir, "json") {
      val df = spark.sql("SELECT * FROM json_extra_cat.default.spark_log_file")
      assert(df.count() === 2)

      val values = df.select("value").collect().map(_.getString(0)).toSet
      assert(values === Set("event A", "event B"))
    }
  }

  test("default format is json when fileFormat not specified") {
    val logDir = resourcePath("json_logs")
    val catalogName = "json_default_cat"
    spark.conf.set(s"spark.sql.catalog.$catalogName",
      classOf[LogFileCatalog].getName)
    spark.conf.set(s"spark.sql.catalog.$catalogName.logDir", logDir)
    try {
      val df = spark.sql(s"SELECT * FROM $catalogName.default.spark_log_file")
      assert(df.count() === 4)
    } finally {
      spark.conf.unset(s"spark.sql.catalog.$catalogName")
      spark.conf.unset(s"spark.sql.catalog.$catalogName.logDir")
    }
  }

  test("file format is case insensitive") {
    val logDir = resourcePath("json_logs")
    withCatalog("json_uppercase_format_cat", logDir, "JSON") {
      assert(spark.sql(
        "SELECT * FROM json_uppercase_format_cat.default.spark_log_file").count() === 4)
    }
  }

  test("reject unsupported file format instead of falling back to text") {
    val logDir = resourcePath("json_logs")
    withCatalog("json_invalid_format_cat", logDir, "jsno") {
      val error = intercept[IllegalArgumentException] {
        spark.sql("SELECT * FROM json_invalid_format_cat.default.spark_log_file")
      }
      assert(error.getMessage.contains("Unsupported fileFormat 'jsno'"))
      assert(error.getMessage.contains("csv, json, text, tfile"))
    }
  }

  test("read log files from eventlog_v2 directory structure") {
    val logDir = resourcePath("eventlog_v2_logs")
    withCatalog("json_v2_cat", logDir, "json") {
      val df = spark.sql("SELECT * FROM json_v2_cat.default.spark_log_file")
      assert(df.count() === 3)

      val appIds = df.select("app_id").distinct().collect().map(_.getString(0)).toSet
      assert(appIds === Set("app_v2_001"))

      val values = df.select("value").collect().map(_.getString(0)).toSet
      assert(values === Set("v2 event 1", "v2 event 2", "v2 event 3"))
    }
  }

  test("read mixed flat files and directory entries") {
    val logDir = resourcePath("mixed_logs")
    withCatalog("json_mixed_cat", logDir, "json") {
      val df = spark.sql("SELECT * FROM json_mixed_cat.default.spark_log_file")
      assert(df.count() === 2)

      val appIds = df.select("app_id").distinct().collect().map(_.getString(0)).toSet
      assert(appIds === Set("app_flat_001", "app_dir_001"))
    }
  }

  test("infer schema from json log files") {
    val logDir = resourcePath("json_extra_logs")
    withCatalog("json_infer_cat", logDir, "json",
      Map("inferSchema" -> "true")) {
      val df = spark.sql("SELECT * FROM json_infer_cat.default.spark_log_file")

      assert(df.columns.toSet === Set("level", "ts", "value", "dt", "hour", "app_id"))
      assert(df.count() === 2)

      val row1 = df.filter("value = 'event A'").select("level", "ts").collect()(0)
      assert(row1.getString(0) === "INFO")
      assert(row1.getString(1) === "2025-01-01")

      val row2 = df.filter("value = 'event B'").select("level", "ts").collect()(0)
      assert(row2.getString(0) === "ERROR")
      assert(row2.isNullAt(1))
    }
  }

  test("inferSchema=false uses default value-only schema for json") {
    val logDir = resourcePath("json_extra_logs")
    withCatalog("json_noinfer_cat", logDir, "json") {
      val df = spark.sql("SELECT * FROM json_noinfer_cat.default.spark_log_file")

      assert(df.columns.toSet === Set("value", "dt", "hour", "app_id"))
      assert(df.count() === 2)

      val values = df.select("value").collect().map(_.getString(0)).toSet
      assert(values === Set("event A", "event B"))
    }
  }

  test("reject inferred data schema that conflicts with partition columns") {
    val logDir = resourcePath("json_collision_logs")
    withCatalog("json_collision_cat", logDir, "json",
      Map("inferSchema" -> "true")) {
      val error = intercept[IllegalArgumentException] {
        spark.sql("SELECT * FROM json_collision_cat.default.spark_log_file")
      }
      assert(error.getMessage.contains("app_id, dt, hour"))
      assert(error.getMessage.contains("reserved partition columns"))
    }
  }

  test("ignore nested inprogress files when inferring schema and reading") {
    val logDir = resourcePath("inprogress_logs")
    withCatalog("json_inprogress_cat", logDir, "json",
      Map("inferSchema" -> "true")) {
      val df = spark.sql("SELECT * FROM json_inprogress_cat.default.spark_log_file")

      assert(df.columns.toSet === Set("value", "dt", "hour", "app_id"))
      assert(df.select("value").collect().map(_.getString(0)).toSet ===
        Set("complete event", "compacted event"))
    }
  }

  test("recursively discover completed plain and rolling log files") {
    val root = Files.createTempDirectory("log-file-recursive-")
    root.toFile.deleteOnExit()

    val rollingDeep = Files.createDirectories(
      root.resolve("eventlog_v2_app_recursive/segments/year/month"))
    writeJson(rollingDeep.resolve("events_1"),
      """{"value":"rolling deep","rolling_field":"kept"}""")
    writeJson(rollingDeep.resolve("appstatus_1"),
      """{"value":"rolling status","status_only":true}""")
    writeJson(rollingDeep.resolve("events_2.inprogress"),
      """{"value":"rolling inprogress","inprogress_only":true}""")

    val pendingDeep = Files.createDirectories(
      root.resolve("eventlog_v2_app_recursive/pending.inprogress/deeper"))
    writeJson(pendingDeep.resolve("events_3"),
      """{"value":"pending directory event","pending_directory_only":true}""")

    val hiddenDeep = Files.createDirectories(
      root.resolve("eventlog_v2_app_recursive/.hidden/deeper"))
    writeJson(hiddenDeep.resolve("events_4"),
      """{"value":"hidden event","hidden_only":true}""")

    val plainDeep = Files.createDirectories(root.resolve("app_plain_recursive/a/b/c"))
    writeJson(plainDeep.resolve("plain-log"),
      """{"value":"plain deep","plain_field":7}""")

    withCatalog("json_recursive_cat", root.toString, "json",
      Map("inferSchema" -> "true")) {
      val df = spark.sql("SELECT * FROM json_recursive_cat.default.spark_log_file")

      assert(df.columns.toSet === Set(
        "value", "rolling_field", "plain_field", "dt", "hour", "app_id"))
      assert(df.select("value").collect().map(_.getString(0)).toSet ===
        Set("rolling deep", "plain deep"))

      val appIds = df.select("app_id").collect().map(_.getString(0)).toSet
      assert(appIds === Set("app_recursive", "app_plain_recursive"))
    }
  }

  private def writeJson(path: Path, json: String): Unit = {
    Files.write(path, (json + "\n").getBytes(StandardCharsets.UTF_8))
      .toFile.deleteOnExit()
  }
}
