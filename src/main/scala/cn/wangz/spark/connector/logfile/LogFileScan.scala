package cn.wangz.spark.connector.logfile

import java.text.SimpleDateFormat
import java.util.{Date, Locale}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.logfile.{
  CsvLogFilePartitionReaderFactory,
  JsonLogFilePartitionReaderFactory,
  TextLogFilePartitionReaderFactory
}
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.sources._
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

    val dateFmt = new SimpleDateFormat("yyyy-MM-dd")
    val hourFmt = new SimpleDateFormat("HH")
    val partitions = new ArrayBuffer[InputPartition]()

    fs.listStatus(logDirPath).foreach { entry =>
      if (LogFileScan.isCompletedLogPath(entry.getPath)) {
        if (entry.isFile) {
          addFilePartition(entry, LogFileScan.extractAppId(entry.getPath.getName),
            dateFmt, hourFmt, partitions)
        } else if (entry.isDirectory) {
          collectDirectoryPartitions(fs, entry, dateFmt, hourFmt, partitions)
        }
      }
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

  /**
   * V2 rolling structure: eventlog_v2_{appId}/{events_*, appstatus_*}
   * Plain directory: {appId}/{log files}
   */
  private def collectDirectoryPartitions(
      fs: FileSystem,
      dirEntry: FileStatus,
      dateFmt: SimpleDateFormat,
      hourFmt: SimpleDateFormat,
      partitions: ArrayBuffer[InputPartition]): Unit = {
    val dirName = dirEntry.getPath.getName
    val isRollingDirectory = dirName.startsWith("eventlog_v2_")
    val appId = if (isRollingDirectory) {
      dirName.stripPrefix("eventlog_v2_")
    } else {
      dirName
    }

    val logFileFilter: Path => Boolean = { p =>
      LogFileScan.isCompletedLogPath(p) &&
        (!isRollingDirectory || p.getName.startsWith("events_"))
    }

    fs.listStatus(dirEntry.getPath, (p: Path) => logFileFilter(p)).foreach { child =>
      if (child.isFile) {
        addFilePartition(child, appId, dateFmt, hourFmt, partitions)
      } else if (child.isDirectory) {
        fs.listStatus(child.getPath, (p: Path) => logFileFilter(p)).foreach { nf =>
          if (nf.isFile) {
            addFilePartition(nf, appId, dateFmt, hourFmt, partitions)
          }
        }
      }
    }
  }

  private def addFilePartition(
      file: FileStatus,
      appId: String,
      dateFmt: SimpleDateFormat,
      hourFmt: SimpleDateFormat,
      partitions: ArrayBuffer[InputPartition]): Unit = {
    val modDate = new Date(file.getModificationTime)
    val dt = dateFmt.format(modDate)
    val hour = hourFmt.format(modDate)
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

  def isCompletedLogPath(path: Path): Boolean = {
    val name = path.getName
    !name.startsWith(".") && !name.startsWith("_") && !name.endsWith(".inprogress")
  }

  def extractAppId(fileName: String): String = {
    var name = fileName
    CodecSuffixes.find(name.endsWith).foreach { suffix =>
      name = name.stripSuffix(suffix)
    }
    name.stripPrefix("eventlog_v2_")
  }
}
