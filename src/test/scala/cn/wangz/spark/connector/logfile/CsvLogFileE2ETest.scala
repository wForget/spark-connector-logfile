package cn.wangz.spark.connector.logfile

class CsvLogFileE2ETest extends LogFileTestBase {

  test("read csv log files via catalog") {
    val logDir = resourcePath("csv_logs")
    withCatalog("csv_cat", logDir, "csv") {
      val df = spark.sql("SELECT * FROM csv_cat.default.spark_log_file")

      assert(df.columns.toSet === Set("value", "dt", "hour", "app_id"))
      assert(df.count() === 5)

      val app1Values = df.filter("app_id = 'app_csv_001'")
        .select("value").collect().map(_.getString(0)).toSet
      assert(app1Values === Set("csv line 1", "csv line 2", "csv line 3"))

      val app2Values = df.filter("app_id = 'app_csv_002'")
        .select("value").collect().map(_.getString(0)).toSet
      assert(app2Values === Set("csv line 4", "csv line 5"))
    }
  }
}
