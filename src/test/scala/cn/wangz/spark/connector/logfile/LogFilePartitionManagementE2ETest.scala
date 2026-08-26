package cn.wangz.spark.connector.logfile

import java.nio.charset.StandardCharsets
import java.nio.file.attribute.FileTime
import java.nio.file.{Files, Path}
import java.time.Instant

class LogFilePartitionManagementE2ETest extends LogFileTestBase {

  test("show distinct logical partitions and filter a partial partition spec") {
    val root = Files.createTempDirectory("log-file-show-partitions-")
    root.toFile.deleteOnExit()
    val rolling = Files.createDirectories(root.resolve("eventlog_v2_app_rolling"))
    rolling.toFile.deleteOnExit()

    val first = write(rolling.resolve("events_1"), "first")
    val second = write(rolling.resolve("events_2"), "second")
    write(rolling.resolve("appstatus_app_rolling"), "ignored")
    write(rolling.resolve("events_3.inprogress"), "ignored")
    val flat = write(root.resolve("app_flat"), "flat")

    val rollingTime = FileTime.from(Instant.parse("2025-01-02T03:00:00Z"))
    Files.setLastModifiedTime(first, rollingTime)
    Files.setLastModifiedTime(second, rollingTime)
    Files.setLastModifiedTime(flat, FileTime.from(Instant.parse("2025-01-03T04:00:00Z")))

    withCatalog("show_partitions_cat", root.toString, "json",
      Map("partitionTimeZone" -> "UTC")) {
      val allRows = spark.sql(
        "SHOW PARTITIONS show_partitions_cat.default.spark_log_file")
        .collect().map(_.getString(0)).toSeq
      assert(allRows.length === 2)
      assert(allRows.toSet === Set(
        "dt=2025-01-02/hour=03/app_id=app_rolling",
        "dt=2025-01-03/hour=04/app_id=app_flat"))

      val filtered = spark.sql(
        """SHOW PARTITIONS show_partitions_cat.default.spark_log_file
          |PARTITION (app_id = 'app_rolling')""".stripMargin)
        .collect().map(_.getString(0)).toSeq
      assert(filtered === Seq("dt=2025-01-02/hour=03/app_id=app_rolling"))

      val selected = spark.sql(
        "SELECT DISTINCT dt, hour, app_id " +
          "FROM show_partitions_cat.default.spark_log_file")
        .collect().map(row => (row.getString(0), row.getString(1), row.getString(2))).toSet
      assert(selected === Set(
        ("2025-01-02", "03", "app_rolling"),
        ("2025-01-03", "04", "app_flat")))
    }
  }

  test("show partitions returns empty for a missing log directory") {
    val missing = Files.createTempDirectory("log-file-missing-partitions-")
      .resolve("does-not-exist")
    withCatalog("missing_partitions_cat", missing.toString, "text") {
      assert(spark.sql(
        "SHOW PARTITIONS missing_partitions_cat.default.spark_log_file").collect().isEmpty)
    }
  }

  test("show partitions validates the time zone even when the directory is missing") {
    val missing = Files.createTempDirectory("log-file-invalid-zone-partitions-")
      .resolve("does-not-exist")
    withCatalog("invalid_zone_partitions_cat", missing.toString, "text",
      Map("partitionTimeZone" -> "not/a-zone")) {
      val error = intercept[IllegalArgumentException] {
        spark.sql(
          "SHOW PARTITIONS invalid_zone_partitions_cat.default.spark_log_file").collect()
      }
      assert(error.getMessage.contains("Invalid partitionTimeZone 'not/a-zone'"))
    }
  }

  private def write(path: Path, value: String): Path = {
    val jsonLine = s"""{"value":"$value"}""" + "\n"
    Files.write(path, jsonLine.getBytes(StandardCharsets.UTF_8))
    path.toFile.deleteOnExit()
    path
  }
}
