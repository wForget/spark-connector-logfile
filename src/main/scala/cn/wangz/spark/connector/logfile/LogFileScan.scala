package cn.wangz.spark.connector.logfile

import java.text.SimpleDateFormat
import java.util.Date

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
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
    pushedFilters: Array[Filter]) extends Scan with Batch {

  private val logDir: String = {
    val dir = options.get("logDir")
    require(dir != null && dir.nonEmpty,
      "logDir is required. Set spark.sql.catalog.<name>.logDir")
    dir
  }

  private val fileFormat: String = options.getOrDefault("fileFormat", "json")

  private val hadoopOptions: Map[String, String] = {
    options.asCaseSensitiveMap().asScala
      .filter(_._1.startsWith("hadoop."))
      .map { case (k, v) => k.stripPrefix("hadoop.") -> v }
      .toMap
  }

  override def readSchema(): StructType = LogFileTable.SCHEMA

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
      val name = entry.getPath.getName
      if (!name.startsWith(".") && !name.startsWith("_") && !name.endsWith(".inprogress")) {
        val modDate = new Date(entry.getModificationTime)
        val dt = dateFmt.format(modDate)
        val hour = hourFmt.format(modDate)

        if (entry.isFile) {
          val appId = LogFileScan.extractAppId(name)
          if (matchesFilters(dt, hour, appId)) {
            partitions += LogFilePartition(
              entry.getPath.toString, appId, dt, hour, entry.getLen)
          }
        } else if (entry.isDirectory) {
          collectDirectoryPartitions(fs, entry, dt, hour, partitions)
        }
      }
    }

    partitions.toArray
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    val format = fileFormat.toLowerCase
    format match {
      case "tfile" =>
        new TFileLogFilePartitionReaderFactory(hadoopOptions)
      case _ =>
        val spark = SparkSession.active
        val sqlConf = spark.sessionState.conf
        val broadcastedConf = spark.sparkContext.broadcast(
          new SerializableConfiguration(buildHadoopConf()))
        val params = options.asCaseSensitiveMap().asScala.toMap
        format match {
          case "json" =>
            JsonLogFilePartitionReaderFactory(
              sqlConf, broadcastedConf,
              LogFileTable.DATA_SCHEMA, LogFileTable.DATA_SCHEMA,
              LogFileTable.PARTITION_SCHEMA, params)
          case "csv" =>
            CsvLogFilePartitionReaderFactory(
              sqlConf, broadcastedConf,
              LogFileTable.DATA_SCHEMA, LogFileTable.DATA_SCHEMA,
              LogFileTable.PARTITION_SCHEMA, params)
          case _ =>
            TextLogFilePartitionReaderFactory(
              sqlConf, broadcastedConf,
              LogFileTable.DATA_SCHEMA, LogFileTable.PARTITION_SCHEMA, params)
        }
    }
  }

  /**
   * V2 rolling structure: eventlog_v2_{appId}/{events_*, appstatus_*.compact}
   * Plain directory: {appId}/{log files}
   */
  private def collectDirectoryPartitions(
      fs: FileSystem,
      dirEntry: FileStatus,
      dt: String,
      hour: String,
      partitions: ArrayBuffer[InputPartition]): Unit = {
    val dirName = dirEntry.getPath.getName
    val appId = if (dirName.startsWith("eventlog_v2_")) {
      dirName.stripPrefix("eventlog_v2_")
    } else {
      dirName
    }

    if (!matchesFilters(dt, hour, appId)) return

    val logFileFilter: Path => Boolean = { p =>
      val n = p.getName
      !n.startsWith(".") && !n.startsWith("_") && !n.endsWith(".compact")
    }

    fs.listStatus(dirEntry.getPath, (p: Path) => logFileFilter(p)).foreach { child =>
      if (child.isFile) {
        partitions += LogFilePartition(
          child.getPath.toString, appId, dt, hour, child.getLen)
      } else if (child.isDirectory) {
        fs.listStatus(child.getPath, (p: Path) => logFileFilter(p)).foreach { nf =>
          if (nf.isFile) {
            partitions += LogFilePartition(
              nf.getPath.toString, appId, dt, hour, nf.getLen)
          }
        }
      }
    }
  }

  private def buildHadoopConf(): Configuration = {
    val conf = try {
      SparkSession.active.sparkContext.hadoopConfiguration
    } catch {
      case _: Exception => new Configuration()
    }
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

  def extractAppId(fileName: String): String = {
    var name = fileName
    CodecSuffixes.find(name.endsWith).foreach { suffix =>
      name = name.stripSuffix(suffix)
    }
    name.stripPrefix("eventlog_v2_")
  }
}
