package cn.wangz.spark.connector.logfile

import java.nio.charset.StandardCharsets
import java.nio.file.attribute.FileTime
import java.nio.file.{Files, Path}
import java.time.{Instant, ZoneId, ZonedDateTime}
import java.util.TimeZone

class LogFilePartitionTimeE2ETest extends LogFileTestBase {

  test("derive dt and hour from each leaf log file") {
    val root = Files.createTempDirectory("log-file-partition-time-")
    root.toFile.deleteOnExit()

    val rollingDir = Files.createDirectories(root.resolve("eventlog_v2_app_time"))
    rollingDir.toFile.deleteOnExit()
    val first = write(rollingDir.resolve("events_1"), "first event")
    val second = write(rollingDir.resolve("events_2"), "second event")

    val plainDir = Files.createDirectories(root.resolve("app_nested_time"))
    plainDir.toFile.deleteOnExit()
    val nestedDir = Files.createDirectories(plainDir.resolve("nested"))
    nestedDir.toFile.deleteOnExit()
    val nested = write(nestedDir.resolve("events_3"), "nested event")

    setModificationTime(first, timestamp(2025, 1, 2, 3))
    setModificationTime(second, timestamp(2025, 1, 3, 4))
    setModificationTime(nested, timestamp(2025, 1, 4, 5))
    setModificationTime(rollingDir, timestamp(2025, 1, 9, 9))
    setModificationTime(plainDir, timestamp(2025, 1, 10, 10))
    setModificationTime(nestedDir, timestamp(2025, 1, 11, 11))

    withSessionTimeZone("UTC") {
      withCatalog("partition_time_cat", root.toString, "json") {
        val df = spark.sql("SELECT * FROM partition_time_cat.default.spark_log_file")
        val actual = df.select("value", "dt", "hour", "app_id").collect().map { row =>
          row.getString(0) -> (row.getString(1), row.getString(2), row.getString(3))
        }.toMap

        assert(actual.size === 3)
        assert(actual("first event") === ("2025-01-02", "03", "app_time"))
        assert(actual("second event") === ("2025-01-03", "04", "app_time"))
        assert(actual("nested event") === ("2025-01-04", "05", "app_nested_time"))

        val filteredValues = df.filter("dt = '2025-01-03' AND hour = '04'")
          .select("value").collect().map(_.getString(0)).toSeq
        assert(filteredValues === Seq("second event"))
      }
    }
  }

  test("use session time zone by default and allow an explicit override") {
    val root = Files.createTempDirectory("log-file-partition-zone-")
    root.toFile.deleteOnExit()
    val logFile = write(root.resolve("app_zone"), "zone event")
    setModificationTime(logFile, Instant.parse("2025-01-01T16:30:00Z").toEpochMilli)

    withDefaultTimeZone("America/Los_Angeles") {
      withSessionTimeZone("UTC") {
        withCatalog("partition_session_zone_cat", root.toString, "json") {
          val row = spark.sql(
            "SELECT dt, hour FROM partition_session_zone_cat.default.spark_log_file").head()
          assert(row.getString(0) === "2025-01-01")
          assert(row.getString(1) === "16")
        }

        withCatalog("partition_explicit_zone_cat", root.toString, "json",
          Map("partitionTimeZone" -> "Asia/Shanghai")) {
          val row = spark.sql(
            "SELECT dt, hour FROM partition_explicit_zone_cat.default.spark_log_file").head()
          assert(row.getString(0) === "2025-01-02")
          assert(row.getString(1) === "00")
        }

        withCatalog("partition_runtime_zone_cat", root.toString, "json",
          Map("partitionTimeZone" -> "America/Los_Angeles")) {
          val df = spark.read
            .option("PaRtItIoNtImEzOnE", "+8:00")
            .table("partition_runtime_zone_cat.default.spark_log_file")

          val rows = df.filter("dt = '2025-01-02' AND hour = '00'")
            .select("value", "dt", "hour").collect()
          assert(rows.length === 1)
          assert(rows.head.getString(0) === "zone event")
        }
      }
    }
  }

  test("reject an invalid partition time zone") {
    val root = Files.createTempDirectory("log-file-invalid-partition-zone-")
    root.toFile.deleteOnExit()
    write(root.resolve("app_invalid_zone"), "invalid zone event")

    withCatalog("partition_invalid_zone_cat", root.toString, "json") {
      Seq("", "not/a-zone").foreach { invalidZone =>
        val error = intercept[IllegalArgumentException] {
          spark.read
            .option("partitionTimeZone", invalidZone)
            .table("partition_invalid_zone_cat.default.spark_log_file")
            .collect()
        }
        assert(error.getMessage.contains(s"Invalid partitionTimeZone '$invalidZone'"))
      }
    }
  }

  private def write(path: Path, value: String): Path = {
    val jsonLine = s"""{"value":"$value"}""" + "\n"
    Files.write(path, jsonLine.getBytes(StandardCharsets.UTF_8))
    path.toFile.deleteOnExit()
    path
  }

  private def timestamp(year: Int, month: Int, day: Int, hour: Int): Long =
    ZonedDateTime.of(year, month, day, hour, 0, 0, 0, ZoneId.of("UTC"))
      .toInstant.toEpochMilli

  private def setModificationTime(path: Path, millis: Long): Unit =
    Files.setLastModifiedTime(path, FileTime.fromMillis(millis))

  private def withSessionTimeZone(timeZone: String)(body: => Unit): Unit = {
    val previous = spark.conf.getOption("spark.sql.session.timeZone")
    spark.conf.set("spark.sql.session.timeZone", timeZone)
    try {
      body
    } finally {
      previous match {
        case Some(value) => spark.conf.set("spark.sql.session.timeZone", value)
        case None => spark.conf.unset("spark.sql.session.timeZone")
      }
    }
  }

  private def withDefaultTimeZone(timeZone: String)(body: => Unit): Unit = {
    val previous = TimeZone.getDefault
    TimeZone.setDefault(TimeZone.getTimeZone(timeZone))
    try {
      body
    } finally {
      TimeZone.setDefault(previous)
    }
  }
}
