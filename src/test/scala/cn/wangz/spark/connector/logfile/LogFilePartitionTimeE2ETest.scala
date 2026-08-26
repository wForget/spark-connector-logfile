package cn.wangz.spark.connector.logfile

import java.nio.charset.StandardCharsets
import java.nio.file.attribute.FileTime
import java.nio.file.{Files, Path}
import java.time.{ZoneId, ZonedDateTime}

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

  private def write(path: Path, value: String): Path = {
    val jsonLine = s"""{"value":"$value"}""" + "\n"
    Files.write(path, jsonLine.getBytes(StandardCharsets.UTF_8))
    path.toFile.deleteOnExit()
    path
  }

  private def timestamp(year: Int, month: Int, day: Int, hour: Int): Long =
    ZonedDateTime.of(year, month, day, hour, 0, 0, 0, ZoneId.systemDefault())
      .toInstant.toEpochMilli

  private def setModificationTime(path: Path, millis: Long): Unit =
    Files.setLastModifiedTime(path, FileTime.fromMillis(millis))
}
