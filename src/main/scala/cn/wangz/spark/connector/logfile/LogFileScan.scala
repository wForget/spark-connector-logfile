package cn.wangz.spark.connector.logfile

import java.time.{DateTimeException, Instant, ZoneId}
import java.time.format.DateTimeFormatter
import java.util.Locale

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, HashSet}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path, PathFilter}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.connector.logfile.{
  CsvLogFilePartitionReaderFactory,
  JsonLogFilePartitionReaderFactory,
  TextLogFilePartitionReaderFactory
}
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.SerializableConfiguration

class LogFileScan(
    options: CaseInsensitiveStringMap,
    dataSchema: StructType,
    pushedFilters: Array[Filter],
    fileFormat: String) extends Scan with Batch {

  private val logDir: String = {
    val dir = options.get("logDir")
    require(dir != null && dir.nonEmpty,
      "logDir is required. Set spark.sql.catalog.<name>.logDir")
    dir
  }

  private val partitionTimeZone: ZoneId = LogFileScan.resolvePartitionTimeZone(options)

  override def readSchema(): StructType =
    new StructType(dataSchema.fields ++ LogFileTable.PARTITION_SCHEMA.fields)

  override def description(): String = s"LogFileScan[$logDir, format=$fileFormat]"

  override def toBatch(): Batch = this

  override def planInputPartitions(): Array[InputPartition] = {
    val hadoopConf = buildHadoopConf()
    val logDirPath = new Path(logDir)
    val fs = logDirPath.getFileSystem(hadoopConf)

    if (!fs.exists(logDirPath)) {
      return Array.empty
    }

    val partitions = new ArrayBuffer[InputPartition]()

    LogFileScan.listLogFiles(fs, logDirPath, pushedFilters, partitionTimeZone)
      .foreach { case (file, appId) =>
        addFilePartition(file, appId, partitions)
      }

    partitions.toArray
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    fileFormat match {
      case "tfile" =>
        val spark = SparkSession.active
        val broadcastedConf: Broadcast[SerializableConfiguration] =
          spark.sparkContext.broadcast(
            new SerializableConfiguration(buildHadoopConf()))
        val params = options.asCaseSensitiveMap().asScala.toMap
        new TFileLogFilePartitionReaderFactory(broadcastedConf, params)
      case "json" | "csv" | "text" =>
        val spark = SparkSession.active
        val sqlConf = spark.sessionState.conf
        val broadcastedConf = spark.sparkContext.broadcast(
          new SerializableConfiguration(buildHadoopConf()))
        val params = options.asCaseSensitiveMap().asScala.toMap
        fileFormat match {
          case "json" =>
            JsonLogFilePartitionReaderFactory(
              sqlConf, broadcastedConf,
              dataSchema, dataSchema,
              LogFileTable.PARTITION_SCHEMA, params)
          case "csv" =>
            CsvLogFilePartitionReaderFactory(
              sqlConf, broadcastedConf,
              dataSchema, dataSchema,
              LogFileTable.PARTITION_SCHEMA, params)
          case "text" =>
            TextLogFilePartitionReaderFactory(
              sqlConf, broadcastedConf,
              dataSchema, LogFileTable.PARTITION_SCHEMA, params)
        }
      case unsupported =>
        throw new IllegalStateException(s"Unsupported normalized fileFormat: $unsupported")
    }
  }

  private def addFilePartition(
      file: FileStatus,
      appId: String,
      partitions: ArrayBuffer[InputPartition]): Unit = {
    val (dt, hour) = LogFileScan.partitionTime(file, partitionTimeZone)
    partitions += LogFilePartition(file.getPath.toString, appId, dt, hour, file.getLen)
  }

  private def buildHadoopConf(): Configuration = LogFileScan.buildHadoopConf(options)
}

object LogFileScan {
  private val CodecSuffixes = Seq(".lz4", ".snappy", ".zstd", ".lzf", ".gz", ".bz2")
  private val HourFormatter = DateTimeFormatter.ofPattern("HH", Locale.ROOT)
  private object CompletedLogPathFilter extends PathFilter {
    override def accept(path: Path): Boolean = isCompletedLogPath(path)
  }

  def isCompletedLogPath(path: Path): Boolean = {
    val name = path.getName
    !name.startsWith(".") && !name.startsWith("_") && !name.endsWith(".inprogress")
  }

  private[logfile] def buildHadoopConf(options: CaseInsensitiveStringMap): Configuration = {
    val base = try {
      SparkSession.active.sparkContext.hadoopConfiguration
    } catch {
      case _: Exception => new Configuration()
    }
    val conf = new Configuration(base)
    options.asCaseSensitiveMap().asScala
      .filter { case (key, _) => key.toLowerCase(Locale.ROOT).startsWith("hadoop.") }
      .foreach { case (key, value) => conf.set(key.substring("hadoop.".length), value) }
    conf
  }

  private[logfile] def resolvePartitionTimeZone(
      options: CaseInsensitiveStringMap): ZoneId = {
    val sessionTimeZone = SQLConf.get.sessionLocalTimeZone
    val configuredTimeZone = options.getOrDefault("partitionTimeZone", sessionTimeZone)
    try {
      DateTimeUtils.getZoneId(configuredTimeZone)
    } catch {
      case error: DateTimeException =>
        throw new IllegalArgumentException(
          s"Invalid partitionTimeZone '$configuredTimeZone'", error)
    }
  }

  private[logfile] def partitionTime(file: FileStatus, timeZone: ZoneId): (String, String) = {
    val modifiedAt = Instant.ofEpochMilli(file.getModificationTime).atZone(timeZone)
    DateTimeFormatter.ISO_LOCAL_DATE.format(modifiedAt) -> HourFormatter.format(modifiedAt)
  }

  /**
   * Lists completed log files below the configured root. The root entry determines app_id and
   * whether Spark's rolling event-log naming rules apply to every descendant file.
   */
  def listLogFiles(fs: FileSystem, logDirPath: Path): Seq[(FileStatus, String)] =
    listLogFiles(fs, logDirPath, Array.empty, ZoneId.systemDefault())

  /**
   * Lists completed log files and applies partition filters before returning scan candidates.
   * Path-only filtering happens inside FileSystem.listStatus; filters that require FileStatus
   * metadata are evaluated as soon as each leaf status is available.
   */
  def listLogFiles(
      fs: FileSystem,
      logDirPath: Path,
      pushedFilters: Array[Filter],
      partitionTimeZone: ZoneId): Seq[(FileStatus, String)] = {
    val discovered = new ArrayBuffer[(FileStatus, String)]()
    val partitionFilter = new PartitionFilter(pushedFilters, partitionTimeZone)

    fs.listStatus(logDirPath, CompletedLogPathFilter).foreach { entry =>
      if (entry.isFile) {
        val appId = extractAppId(entry.getPath.getName)
        if (partitionFilter.matchesFile(entry, appId)) {
          discovered += entry -> appId
        }
      } else if (entry.isDirectory && !entry.isSymlink) {
        val dirName = entry.getPath.getName
        val isRollingDirectory = dirName.startsWith("eventlog_v2_")
        val appId = if (isRollingDirectory) {
          dirName.stripPrefix("eventlog_v2_")
        } else {
          dirName
        }
        if (partitionFilter.matchesAppId(appId)) {
          collectDirectoryLogFiles(
              fs, entry.getPath, appId, isRollingDirectory, partitionFilter).foreach { file =>
            discovered += file -> appId
          }
        }
      }
    }

    discovered.sortBy { case (file, _) => file.getPath.toString }.toSeq
  }

  private def collectDirectoryLogFiles(
      fs: FileSystem,
      root: Path,
      appId: String,
      isRollingDirectory: Boolean,
      partitionFilter: PartitionFilter): Seq[FileStatus] = {
    val pendingDirectories = ArrayBuffer(root)
    val visitedDirectories = HashSet.empty[String]
    val files = new ArrayBuffer[FileStatus]()

    while (pendingDirectories.nonEmpty) {
      val directory = pendingDirectories.remove(pendingDirectories.length - 1)
      val qualifiedDirectory = fs.makeQualified(directory).toUri.normalize().toString
      if (visitedDirectories.add(qualifiedDirectory)) {
        fs.listStatus(directory, CompletedLogPathFilter).foreach { entry =>
          if (entry.isDirectory && !entry.isSymlink) {
            pendingDirectories += entry.getPath
          } else if (entry.isFile &&
              (!isRollingDirectory || entry.getPath.getName.startsWith("events_")) &&
              partitionFilter.matchesFile(entry, appId)) {
            files += entry
          }
        }
      }
    }

    files.sortBy(_.getPath.toString).toSeq
  }

  private final class PartitionFilter(
      filters: Array[Filter],
      partitionTimeZone: ZoneId) {

    private val needsFileTime = filters.exists { filter =>
      filterAttribute(filter).exists { attribute =>
        val normalized = attribute.toLowerCase(Locale.ROOT)
        normalized == "dt" || normalized == "hour"
      }
    }

    def matchesAppId(appId: String): Boolean =
      matchesFilters(None, None, Some(appId))

    def matchesFile(file: FileStatus, appId: String): Boolean = {
      if (!matchesAppId(appId)) {
        false
      } else if (!needsFileTime) {
        true
      } else {
        val modifiedAt = Instant.ofEpochMilli(file.getModificationTime).atZone(partitionTimeZone)
        val dt = DateTimeFormatter.ISO_LOCAL_DATE.format(modifiedAt)
        val hour = HourFormatter.format(modifiedAt)
        matchesFilters(Some(dt), Some(hour), Some(appId))
      }
    }

    private def matchesFilters(
        dt: Option[String],
        hour: Option[String],
        appId: Option[String]): Boolean =
      filters.forall(matchesFilter(_, dt, hour, appId))

    private def matchesFilter(
        filter: Filter,
        dt: Option[String],
        hour: Option[String],
        appId: Option[String]): Boolean = {
      val partitionValue = filterAttribute(filter).flatMap { attribute =>
        attribute.toLowerCase(Locale.ROOT) match {
          case "dt"     => dt
          case "hour"   => hour
          case "app_id" => appId
          case _        => None
        }
      }

      filter match {
        case EqualTo(_, value) =>
          partitionValue.forall(_ == String.valueOf(value))
        case In(_, values) =>
          partitionValue.forall(v => values.exists(x => v == String.valueOf(x)))
        case StringStartsWith(_, value) =>
          partitionValue.forall(_.startsWith(String.valueOf(value)))
        case GreaterThan(_, value) =>
          partitionValue.forall(_ > String.valueOf(value))
        case GreaterThanOrEqual(_, value) =>
          partitionValue.forall(_ >= String.valueOf(value))
        case LessThan(_, value) =>
          partitionValue.forall(_ < String.valueOf(value))
        case LessThanOrEqual(_, value) =>
          partitionValue.forall(_ <= String.valueOf(value))
        case _ => true
      }
    }
  }

  private def filterAttribute(filter: Filter): Option[String] = filter match {
    case f: EqualTo            => Some(f.attribute)
    case f: In                 => Some(f.attribute)
    case f: StringStartsWith   => Some(f.attribute)
    case f: GreaterThan        => Some(f.attribute)
    case f: GreaterThanOrEqual => Some(f.attribute)
    case f: LessThan           => Some(f.attribute)
    case f: LessThanOrEqual    => Some(f.attribute)
    case _                     => None
  }

  def extractAppId(fileName: String): String = {
    var name = fileName
    CodecSuffixes.find(name.endsWith).foreach { suffix =>
      name = name.stripSuffix(suffix)
    }
    name.stripPrefix("eventlog_v2_")
  }
}
