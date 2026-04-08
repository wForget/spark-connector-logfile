package cn.wangz.spark.connector.logfile

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
}
