package cn.wangz.spark.connector.logfile

import java.time.{DateTimeException, Instant, ZoneId}
import java.time.format.DateTimeFormatter
import java.util.Locale

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, HashSet}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
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

  private val hadoopOptions: Map[String, String] = {
    options.asCaseSensitiveMap().asScala
      .filter { case (k, _) => k.toLowerCase(Locale.ROOT).startsWith("hadoop.") }
      .map { case (k, v) => k.substring("hadoop.".length) -> v }
      .toMap
  }

  private val partitionTimeZone: ZoneId = {
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

    LogFileScan.listLogFiles(fs, logDirPath).foreach { case (file, appId) =>
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
    val modifiedAt = Instant.ofEpochMilli(file.getModificationTime).atZone(partitionTimeZone)
    val dt = DateTimeFormatter.ISO_LOCAL_DATE.format(modifiedAt)
    val hour = LogFileScan.HourFormatter.format(modifiedAt)
    if (matchesFilters(dt, hour, appId)) {
      partitions += LogFilePartition(file.getPath.toString, appId, dt, hour, file.getLen)
    }
  }

  private def buildHadoopConf(): Configuration = {
    val base = try {
      SparkSession.active.sparkContext.hadoopConfiguration
    } catch {
      case _: Exception => new Configuration()
    }
    val conf = new Configuration(base)
    hadoopOptions.foreach { case (k, v) => conf.set(k, v) }
    conf
  }

  private def matchesFilters(dt: String, hour: String, appId: String): Boolean =
    pushedFilters.forall(matchesFilter(_, dt, hour, appId))

  private def matchesFilter(filter: Filter, dt: String, hour: String, appId: String): Boolean = {
    def pv(attr: String): Option[String] = attr.toLowerCase match {
      case "dt"     => Some(dt)
      case "hour"   => Some(hour)
      case "app_id" => Some(appId)
      case _        => None
    }

    filter match {
      case EqualTo(attr, value) =>
        pv(attr).forall(_ == String.valueOf(value))
      case In(attr, values) =>
        pv(attr).forall(v => values.exists(x => v == String.valueOf(x)))
      case StringStartsWith(attr, value) =>
        pv(attr).forall(_.startsWith(String.valueOf(value)))
      case GreaterThan(attr, value) =>
        pv(attr).forall(_ > String.valueOf(value))
      case GreaterThanOrEqual(attr, value) =>
        pv(attr).forall(_ >= String.valueOf(value))
      case LessThan(attr, value) =>
        pv(attr).forall(_ < String.valueOf(value))
      case LessThanOrEqual(attr, value) =>
        pv(attr).forall(_ <= String.valueOf(value))
      case _ => true
    }
  }
}

object LogFileScan {
  private val CodecSuffixes = Seq(".lz4", ".snappy", ".zstd", ".lzf", ".gz", ".bz2")
  private val HourFormatter = DateTimeFormatter.ofPattern("HH", Locale.ROOT)

  def isCompletedLogPath(path: Path): Boolean = {
    val name = path.getName
    !name.startsWith(".") && !name.startsWith("_") && !name.endsWith(".inprogress")
  }

  /**
   * Lists completed log files below the configured root. The root entry determines app_id and
   * whether Spark's rolling event-log naming rules apply to every descendant file.
   */
  def listLogFiles(fs: FileSystem, logDirPath: Path): Seq[(FileStatus, String)] = {
    val discovered = new ArrayBuffer[(FileStatus, String)]()

    fs.listStatus(logDirPath).foreach { entry =>
      if (isCompletedLogPath(entry.getPath)) {
        if (entry.isFile) {
          discovered += entry -> extractAppId(entry.getPath.getName)
        } else if (entry.isDirectory && !entry.isSymlink) {
          val dirName = entry.getPath.getName
          val isRollingDirectory = dirName.startsWith("eventlog_v2_")
          val appId = if (isRollingDirectory) {
            dirName.stripPrefix("eventlog_v2_")
          } else {
            dirName
          }
          collectDirectoryLogFiles(fs, entry.getPath, isRollingDirectory).foreach { file =>
            discovered += file -> appId
          }
        }
      }
    }

    discovered.sortBy { case (file, _) => file.getPath.toString }
  }

  private def collectDirectoryLogFiles(
      fs: FileSystem,
      root: Path,
      isRollingDirectory: Boolean): Seq[FileStatus] = {
    val pendingDirectories = ArrayBuffer(root)
    val visitedDirectories = HashSet.empty[String]
    val files = new ArrayBuffer[FileStatus]()

    while (pendingDirectories.nonEmpty) {
      val directory = pendingDirectories.remove(pendingDirectories.length - 1)
      val qualifiedDirectory = fs.makeQualified(directory).toUri.normalize().toString
      if (visitedDirectories.add(qualifiedDirectory)) {
        fs.listStatus(directory).foreach { entry =>
          if (isCompletedLogPath(entry.getPath)) {
            if (entry.isDirectory && !entry.isSymlink) {
              pendingDirectories += entry.getPath
            } else if (entry.isFile &&
                (!isRollingDirectory || entry.getPath.getName.startsWith("events_"))) {
              files += entry
            }
          }
        }
      }
    }

    files.sortBy(_.getPath.toString)
  }

  def extractAppId(fileName: String): String = {
    var name = fileName
    CodecSuffixes.find(name.endsWith).foreach { suffix =>
      name = name.stripSuffix(suffix)
    }
    name.stripPrefix("eventlog_v2_")
  }
}
