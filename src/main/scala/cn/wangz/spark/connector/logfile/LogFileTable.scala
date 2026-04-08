package cn.wangz.spark.connector.logfile

import java.util

import org.apache.hadoop.fs.FileStatus
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.TableCapability
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.v2.FileTable
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class LogFileTable(
    sparkSession: SparkSession,
    options: CaseInsensitiveStringMap,
    paths: Seq[String],
    userSpecifiedSchema: Option[StructType])
  extends FileTable(sparkSession, options, paths, userSpecifiedSchema) {

  override def name(): String = s"LogFileTable(${paths.mkString(",")})"

  override lazy val schema: StructType = LogFileTable.SCHEMA

  override def capabilities(): util.Set[TableCapability] =
    util.Collections.singleton(TableCapability.BATCH_READ)

  override def newScanBuilder(scanOptions: CaseInsensitiveStringMap): ScanBuilder =
    new LogFileScanBuilder(options)

  override def inferSchema(files: Seq[FileStatus]): Option[StructType] =
    Some(LogFileTable.SCHEMA)

  override def formatName: String = "logfile"

  override def fallbackFileFormat: Class[_ <: FileFormat] =
    throw new UnsupportedOperationException("LogFileTable does not support V1 fallback")

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder =
    throw new UnsupportedOperationException("LogFileTable is read-only")
}

object LogFileTable {
  val SCHEMA: StructType = new StructType()
    .add("value", DataTypes.StringType, nullable = true)
    .add("dt", DataTypes.StringType, nullable = false)
    .add("hour", DataTypes.StringType, nullable = false)
    .add("app_id", DataTypes.StringType, nullable = false)
}
