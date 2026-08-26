package cn.wangz.spark.connector.logfile

import org.apache.spark.sql.AnalysisException

class SparkDependencyCompatibilityTest extends LogFileTestBase {

  test("run against the selected Spark, Scala, and Java compatibility baseline") {
    val baselines = Map(
      "spark-3.5" -> ("3.5.7", "2.12.18", 8),
      "spark-4.2" -> ("4.2.0", "2.13.18", 17))
    val (expectedSpark, expectedScala, expectedJavaRelease) =
      baselines(sys.props("spark.test.profile"))

    assert(sys.props("spark.test.version") === expectedSpark)
    assert(sys.props("scala.test.version") === expectedScala)
    assert(sys.props("java.test.release").toInt === expectedJavaRelease)
    assert(spark.version === expectedSpark)
    assert(scala.util.Properties.versionNumberString === expectedScala)

    val specificationVersion = sys.props("java.specification.version")
    val javaFeatureVersion = if (specificationVersion.startsWith("1.")) {
      specificationVersion.substring(2).takeWhile(_.isDigit).toInt
    } else {
      specificationVersion.takeWhile(_.isDigit).toInt
    }
    assert(javaFeatureVersion >= expectedJavaRelease)
  }

  test("construct Spark analysis errors without dependency linkage failures") {
    val error = intercept[AnalysisException] {
      spark.sql("SELECT missing_column FROM VALUES (1) AS t(value)")
    }
    assert(error.getMessage.contains("missing_column"))
  }
}
