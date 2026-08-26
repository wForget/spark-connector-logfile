package cn.wangz.spark.connector.logfile

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

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

  test("expose Spark input file metadata") {
    val logDir = resourcePath("text_logs")
    withCatalog("text_metadata_cat", logDir, "text") {
      val row = spark.sql(
        """SELECT input_file_name(), input_file_block_start(), input_file_block_length()
          |FROM text_metadata_cat.default.spark_log_file
          |WHERE app_id = 'app_text_001'
          |LIMIT 1""".stripMargin).head()

      assert(row.getString(0).endsWith("/app_text_001"))
      assert(row.getLong(1) === 0L)
      assert(row.getLong(2) === Files.size(Paths.get(logDir).resolve("app_text_001")))
    }
  }

  test("honor ignoreCorruptFiles") {
    val logDir = Files.createTempDirectory("log-file-corrupt-text-")
    logDir.toFile.deleteOnExit()
    Files.write(logDir.resolve("valid"), "valid line\n".getBytes(StandardCharsets.UTF_8))
      .toFile.deleteOnExit()
    Files.write(logDir.resolve("broken.gz"), Array[Byte](1, 2, 3, 4, 5))
      .toFile.deleteOnExit()

    withCatalog("text_corrupt_cat", logDir.toString, "text",
      Map("ignoreCorruptFiles" -> "true")) {
      val values = spark.sql("SELECT value FROM text_corrupt_cat.default.spark_log_file")
        .collect().map(_.getString(0)).toSeq
      assert(values === Seq("valid line"))
    }
  }

  test("honor ignoreMissingFiles after partition planning") {
    val logDir = Files.createTempDirectory("log-file-missing-text-")
    logDir.toFile.deleteOnExit()
    val logFile = Files.write(
      logDir.resolve("missing_after_planning"),
      "planned line\n".getBytes(StandardCharsets.UTF_8))

    withCatalog("text_missing_cat", logDir.toString, "text",
      Map("ignoreMissingFiles" -> "true")) {
      val plannedRdd = spark.sql(
        "SELECT * FROM text_missing_cat.default.spark_log_file").queryExecution.toRdd
      assert(plannedRdd.partitions.length === 1)

      Files.delete(logFile)
      assert(plannedRdd.collect().isEmpty)
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
