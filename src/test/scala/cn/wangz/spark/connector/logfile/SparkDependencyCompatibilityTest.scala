package cn.wangz.spark.connector.logfile

import org.apache.spark.sql.AnalysisException

class SparkDependencyCompatibilityTest extends LogFileTestBase {

  test("construct Spark analysis errors without dependency linkage failures") {
    val error = intercept[AnalysisException] {
      spark.sql("SELECT missing_column FROM VALUES (1) AS t(value)")
    }
    assert(error.getMessage.contains("missing_column"))
  }
}
