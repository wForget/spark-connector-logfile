package cn.wangz.spark.connector.logfile

import java.io.IOException
import java.nio.file.Files

import org.apache.hadoop.conf.Configuration

class TFileLogFileE2ETest extends LogFileTestBase {

  test("read tfile log files via catalog") {
    val logDir = resourcePath("tfile_logs")
    withCatalog("tfile_cat", logDir, "tfile") {
      val df = spark.sql("SELECT * FROM tfile_cat.default.spark_log_file")

      assert(df.columns.toSet === Set("value", "dt", "hour", "app_id"))
      assert(df.count() === 3)

      val values = df.select("value").collect().map(_.getString(0)).toSet
      assert(values === Set("tfile event 1", "tfile event 2", "tfile event 3"))

      val appIds = df.select("app_id").distinct().collect().map(_.getString(0)).toSet
      assert(appIds === Set("app_tfile_001"))
    }
  }

  test("preserve empty and whitespace-only tfile values") {
    val logDir = Files.createTempDirectory("tfile-whitespace-")
    logDir.toFile.deleteOnExit()
    val logFile = logDir.resolve("app_tfile_whitespace")
    logFile.toFile.deleteOnExit()
    val expected = Seq("", "", "   ", "\t", " leading", "trailing ", "\t padded \t")
    TFileTestDataGenerator.generate(logFile.toString, expected)

    withCatalog("tfile_whitespace_cat", logDir.toString, "tfile") {
      val actual = spark.sql(
        "SELECT value FROM tfile_whitespace_cat.default.spark_log_file")
        .collect().map(_.getString(0)).toSeq
      assert(actual.sorted === expected.sorted)
    }
  }

  test("close is idempotent and does not initialize an unread file") {
    val logDir = Files.createTempDirectory("tfile-unread-")
    logDir.toFile.deleteOnExit()
    val missingFile = logDir.resolve("missing")
    val reader = new TFileLogFilePartitionReader(
      missingFile.toString, "app", "2025-01-01", "00", new Configuration())

    reader.close()
    reader.close()
  }

  test("initialization failure is cleaned up without retrying during close") {
    val corruptFile = Files.createTempFile("tfile-corrupt-", ".tfile")
    corruptFile.toFile.deleteOnExit()
    Files.write(corruptFile, Array[Byte](1, 2, 3, 4))
    val reader = new TFileLogFilePartitionReader(
      corruptFile.toString, "app", "2025-01-01", "00", new Configuration())

    intercept[IOException](reader.next())
    reader.close()
    reader.close()
  }
}
