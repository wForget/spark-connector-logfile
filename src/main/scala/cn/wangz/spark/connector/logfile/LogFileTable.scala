package cn.wangz.spark.connector.logfile

import java.util

import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.logfile.LogFileSchemaInference
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class LogFileTable(options: CaseInsensitiveStringMap) extends Table with SupportsRead {

  private lazy val dataSchema: StructType = {
    val shouldInfer = java.lang.Boolean.parseBoolean(
      options.getOrDefault("inferSchema", "false"))
    val format = options.getOrDefault("fileFormat", "json").toLowerCase

    if (shouldInfer && (format == "json" || format == "csv")) {
      LogFileSchemaInference.infer(options, format).getOrElse(LogFileTable.DATA_SCHEMA)
    } else {
      LogFileTable.DATA_SCHEMA
    }
  }

  override def name(): String = "LogFileTable"

  override def schema(): StructType =
    new StructType(dataSchema.fields ++ LogFileTable.PARTITION_SCHEMA.fields)

  override def capabilities(): util.Set[TableCapability] =
    util.Collections.singleton(TableCapability.BATCH_READ)

  override def newScanBuilder(scanOptions: CaseInsensitiveStringMap): ScanBuilder =
    new LogFileScanBuilder(options, dataSchema)
}

object LogFileTable {
  val PARTITION_COLUMNS: Set[String] = Set("dt", "hour", "app_id")

  val DATA_SCHEMA: StructType = new StructType()
    .add("value", DataTypes.StringType, nullable = true)

  val PARTITION_SCHEMA: StructType = new StructType()
    .add("dt", DataTypes.StringType, nullable = false)
    .add("hour", DataTypes.StringType, nullable = false)
    .add("app_id", DataTypes.StringType, nullable = false)

  val SCHEMA: StructType = new StructType(DATA_SCHEMA.fields ++ PARTITION_SCHEMA.fields)
}
