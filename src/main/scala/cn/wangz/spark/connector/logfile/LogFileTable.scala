package cn.wangz.spark.connector.logfile

import java.util
import java.util.Locale

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.logfile.LogFileSchemaInference
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class LogFileTable(options: CaseInsensitiveStringMap) extends Table with SupportsRead {

  private val fileFormat: String = LogFileTable.normalizeFileFormat(
    options.getOrDefault("fileFormat", "json"))

  private lazy val dataSchema: StructType = {
    val explicitSchema = Option(options.get("schema"))
      .map(_.trim)
      .filter(_.nonEmpty)
      .map { ddl =>
        require(fileFormat == "json" || fileFormat == "csv",
          s"Explicit schema is only supported for json and csv, not '$fileFormat'")
        StructType.fromDDL(ddl)
      }
    val shouldInfer = java.lang.Boolean.parseBoolean(
      options.getOrDefault("inferSchema", "false"))

    val resolvedSchema = explicitSchema.orElse {
      if (shouldInfer && (fileFormat == "json" || fileFormat == "csv")) {
        LogFileSchemaInference.infer(options, fileFormat)
      } else {
        None
      }
    }.getOrElse(LogFileTable.DATA_SCHEMA)

    LogFileTable.validateNoPartitionColumnConflicts(resolvedSchema)
    resolvedSchema
  }

  override def name(): String = "LogFileTable"

  override def schema(): StructType =
    new StructType(dataSchema.fields ++ LogFileTable.PARTITION_SCHEMA.fields)

  override def capabilities(): util.Set[TableCapability] =
    util.Collections.singleton(TableCapability.BATCH_READ)

  override def newScanBuilder(scanOptions: CaseInsensitiveStringMap): ScanBuilder = {
    LogFileTable.validateTableLevelScanOptions(options, scanOptions)
    val mergedOptions = LogFileTable.mergeOptions(options, scanOptions)
    new LogFileScanBuilder(mergedOptions, dataSchema, fileFormat)
  }
}

object LogFileTable {
  val SUPPORTED_FILE_FORMATS: Seq[String] = Seq("csv", "json", "text", "tfile")

  val PARTITION_COLUMNS: Set[String] = Set("dt", "hour", "app_id")

  val DATA_SCHEMA: StructType = new StructType()
    .add("value", DataTypes.StringType, nullable = true)

  val PARTITION_SCHEMA: StructType = new StructType()
    .add("dt", DataTypes.StringType, nullable = false)
    .add("hour", DataTypes.StringType, nullable = false)
    .add("app_id", DataTypes.StringType, nullable = false)

  val SCHEMA: StructType = new StructType(DATA_SCHEMA.fields ++ PARTITION_SCHEMA.fields)

  private[logfile] def normalizeFileFormat(fileFormat: String): String = {
    val normalized = Option(fileFormat).map(_.toLowerCase(Locale.ROOT)).getOrElse("")
    require(SUPPORTED_FILE_FORMATS.contains(normalized),
      s"Unsupported fileFormat '$fileFormat'. " +
        s"Supported formats: ${SUPPORTED_FILE_FORMATS.mkString(", ")}")
    normalized
  }

  private[logfile] def mergeOptions(
      base: CaseInsensitiveStringMap,
      overrides: CaseInsensitiveStringMap): CaseInsensitiveStringMap = {
    val merged = mutable.LinkedHashMap.empty[String, (String, String)]

    def addOptions(source: CaseInsensitiveStringMap): Unit = {
      source.asCaseSensitiveMap().asScala.foreach { case (key, value) =>
        merged.update(key.toLowerCase(Locale.ROOT), key -> value)
      }
    }

    addOptions(base)
    addOptions(overrides)
    new CaseInsensitiveStringMap(merged.values.toMap.asJava)
  }

  private[logfile] def validateTableLevelScanOptions(
      catalogOptions: CaseInsensitiveStringMap,
      scanOptions: CaseInsensitiveStringMap): Unit = {
    def rejectIfChanged(key: String, catalogValue: String, normalize: String => String): Unit = {
      Option(scanOptions.get(key)).foreach { scanValue =>
        require(normalize(scanValue) == normalize(catalogValue),
          s"Scan option '$key' cannot override catalog value '$catalogValue' " +
            s"with '$scanValue' because it defines the table schema or location")
      }
    }

    rejectIfChanged("logDir", catalogOptions.get("logDir"), Option(_).getOrElse(""))
    rejectIfChanged("fileFormat", catalogOptions.getOrDefault("fileFormat", "json"),
      normalizeFileFormat)
    rejectIfChanged("inferSchema", catalogOptions.getOrDefault("inferSchema", "false"),
      value => java.lang.Boolean.parseBoolean(value).toString)
    rejectIfChanged("schema", Option(catalogOptions.get("schema")).getOrElse(""),
      normalizeSchemaOption)
  }

  private def normalizeSchemaOption(value: String): String =
    Option(value).map(_.trim).filter(_.nonEmpty)
      .map(StructType.fromDDL(_).json)
      .getOrElse("")

  private[logfile] def validateNoPartitionColumnConflicts(dataSchema: StructType): Unit = {
    val conflicts = dataSchema.fieldNames
      .filter(name => PARTITION_COLUMNS.contains(name.toLowerCase(Locale.ROOT)))
      .distinct
      .sorted
    require(conflicts.isEmpty,
      s"Data schema contains reserved partition columns: ${conflicts.mkString(", ")}. " +
        s"Reserved columns are: ${PARTITION_COLUMNS.toSeq.sorted.mkString(", ")}")
  }
}
