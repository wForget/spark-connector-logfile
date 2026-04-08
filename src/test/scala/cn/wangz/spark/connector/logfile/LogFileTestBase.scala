package cn.wangz.spark.connector.logfile

import java.io.File

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

trait LogFileTestBase extends AnyFunSuite with BeforeAndAfterAll {

  @transient protected var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession.builder()
      .master("local[2]")
      .appName("LogFileE2ETest")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
  }

  protected def resourcePath(name: String): String = {
    val url = getClass.getClassLoader.getResource(name)
    require(url != null, s"Test resource not found: $name")
    new File(url.toURI).getAbsolutePath
  }

  protected def withCatalog(
      catalogName: String,
      logDir: String,
      format: String,
      extraOptions: Map[String, String] = Map.empty)
      (body: => Unit): Unit = {
    spark.conf.set(s"spark.sql.catalog.$catalogName",
      classOf[LogFileCatalog].getName)
    spark.conf.set(s"spark.sql.catalog.$catalogName.logDir", logDir)
    spark.conf.set(s"spark.sql.catalog.$catalogName.fileFormat", format)
    extraOptions.foreach { case (k, v) =>
      spark.conf.set(s"spark.sql.catalog.$catalogName.$k", v)
    }
    try {
      body
    } finally {
      spark.conf.unset(s"spark.sql.catalog.$catalogName")
      spark.conf.unset(s"spark.sql.catalog.$catalogName.logDir")
      spark.conf.unset(s"spark.sql.catalog.$catalogName.fileFormat")
      extraOptions.keys.foreach { k =>
        spark.conf.unset(s"spark.sql.catalog.$catalogName.$k")
      }
    }
  }
}
