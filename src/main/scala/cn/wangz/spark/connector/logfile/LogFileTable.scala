package cn.wangz.spark.connector.logfile

import java.util
import java.util.Locale

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.{SupportsPartitionManagement, SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.expressions.{Expressions, Transform}
import org.apache.spark.sql.connector.logfile.LogFileSchemaInference
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.unsafe.types.UTF8String

class LogFileTable(options: CaseInsensitiveStringMap)
    extends Table with SupportsRead with SupportsPartitionManagement {

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

  override def partitionSchema(): StructType = LogFileTable.PARTITION_SCHEMA

  override def partitioning(): Array[Transform] =
    LogFileTable.PARTITION_COLUMN_NAMES.map(Expressions.identity).toArray

  override def newScanBuilder(scanOptions: CaseInsensitiveStringMap): ScanBuilder = {
    LogFileTable.validateTableLevelScanOptions(options, scanOptions)
    val mergedOptions = LogFileTable.mergeOptions(options, scanOptions)
    new LogFileScanBuilder(mergedOptions, dataSchema, fileFormat)
  }

  override def createPartition(
      ident: InternalRow,
      properties: util.Map[String, String]): Unit =
    throw new UnsupportedOperationException("LogFileTable is read-only")

  override def dropPartition(ident: InternalRow): Boolean =
    throw new UnsupportedOperationException("LogFileTable is read-only")

  override def replacePartitionMetadata(
      ident: InternalRow,
      properties: util.Map[String, String]): Unit =
    throw new UnsupportedOperationException("LogFileTable is read-only")

  override def loadPartitionMetadata(ident: InternalRow): util.Map[String, String] =
    throw new UnsupportedOperationException("LogFileTable is read-only")

  override def listPartitionIdentifiers(
      names: Array[String],
      ident: InternalRow): Array[InternalRow] = {
    require(names != null, "Partition field names are required")
    require(ident != null, "Partition identifier is required")
    require(names.length == ident.numFields,
      s"Partition field count ${names.length} does not match value count ${ident.numFields}")

    val normalizedNames = names.map { name =>
      require(name != null, "Partition field name cannot be null")
      name.toLowerCase(Locale.ROOT)
    }
    require(normalizedNames.distinct.length == normalizedNames.length,
      s"Duplicate partition fields: ${names.mkString(", ")}")
    normalizedNames.foreach { name =>
      require(LogFileTable.PARTITION_COLUMNS.contains(name),
        s"Unknown partition field '$name'. Expected: " +
          LogFileTable.PARTITION_COLUMN_NAMES.mkString(", "))
    }

    val requestedValues = normalizedNames.indices.map { index =>
      normalizedNames(index) ->
        (if (ident.isNullAt(index)) None else Some(ident.getString(index)))
    }

    listPartitionValues()
      .filter { case (dt, hour, appId) =>
        val partitionValues = Map("dt" -> dt, "hour" -> hour, "app_id" -> appId)
        requestedValues.forall {
          case (_, None) => false
          case (name, Some(value)) => partitionValues(name) == value
        }
      }
      .map { case (dt, hour, appId) =>
        InternalRow(
          UTF8String.fromString(dt),
          UTF8String.fromString(hour),
          UTF8String.fromString(appId))
      }
      .toArray
  }

  private def listPartitionValues(): Seq[(String, String, String)] = {
    val logDir = options.get("logDir")
    require(logDir != null && logDir.nonEmpty,
      "logDir is required. Set spark.sql.catalog.<name>.logDir")
    val root = new Path(logDir)
    val hadoopConf = LogFileScan.buildHadoopConf(options)
    val fs = root.getFileSystem(hadoopConf)
    val timeZone = LogFileScan.resolvePartitionTimeZone(options)
    if (!fs.exists(root)) {
      Seq.empty
    } else {
      LogFileScan.listLogFiles(fs, root).map { case (file, appId) =>
        val (dt, hour) = LogFileScan.partitionTime(file, timeZone)
        (dt, hour, appId)
      }.distinct.sorted
    }
  }
}

object LogFileTable {
  val SUPPORTED_FILE_FORMATS: Seq[String] = Seq("csv", "json", "text", "tfile")

  val PARTITION_COLUMN_NAMES: Seq[String] = Seq("dt", "hour", "app_id")

  val PARTITION_COLUMNS: Set[String] = PARTITION_COLUMN_NAMES.toSet

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
