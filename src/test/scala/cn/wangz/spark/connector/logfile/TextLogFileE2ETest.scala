package cn.wangz.spark.connector.logfile

class TextLogFileE2ETest extends LogFileTestBase {

  test("read text log files via catalog") {
    val logDir = resourcePath("text_logs")
    withCatalog("text_cat", logDir, "text") {
      val df = spark.sql("SELECT * FROM text_cat.default.spark_log_file")

      assert(df.columns.toSet === Set("value", "dt", "hour", "app_id"))
      assert(df.count() === 5)

      val appIds = df.select("app_id").distinct().collect().map(_.getString(0)).toSet
      assert(appIds === Set("app_text_001", "app_text_002"))

      val values = df.filter("app_id = 'app_text_001'")
        .select("value").collect().map(_.getString(0)).toSet
      assert(values === Set(
        "INFO Starting application",
        "WARN Low memory",
        "ERROR OutOfMemoryError"
      ))
    }
  }

  test("read text log files with partition filter") {
    val logDir = resourcePath("text_logs")
    withCatalog("text_filter_cat", logDir, "text") {
      val df = spark.sql(
        "SELECT * FROM text_filter_cat.default.spark_log_file WHERE app_id = 'app_text_001'")
      assert(df.count() === 3)
      assert(df.select("app_id").distinct().collect()(0).getString(0) === "app_text_001")
    }
  }

  test("schema matches expected columns and types") {
    val logDir = resourcePath("text_logs")
    withCatalog("text_schema_cat", logDir, "text") {
      val df = spark.sql("SELECT * FROM text_schema_cat.default.spark_log_file")
      assert(df.schema === LogFileTable.SCHEMA)
    }
  }

  test("read from empty log directory returns zero rows") {
    val logDir = resourcePath("empty_logs")
    withCatalog("text_empty_cat", logDir, "text") {
      val df = spark.sql("SELECT * FROM text_empty_cat.default.spark_log_file")
      assert(df.count() === 0)
    }
  }

  test("read from non-existent directory returns zero rows") {
    val logDir = resourcePath("text_logs") + "/no_such_dir"
    withCatalog("text_nodir_cat", logDir, "text") {
      val df = spark.sql("SELECT * FROM text_nodir_cat.default.spark_log_file")
      assert(df.count() === 0)
    }
  }
}
