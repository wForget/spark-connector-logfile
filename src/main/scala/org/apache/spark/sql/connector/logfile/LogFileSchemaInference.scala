package org.apache.spark.sql.connector.logfile

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.csv.CSVOptions
import org.apache.spark.sql.catalyst.json.JSONOptionsInRead
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.datasources.csv.CSVDataSource
import org.apache.spark.sql.execution.datasources.json.JsonDataSource
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

object LogFileSchemaInference {

  def infer(options: CaseInsensitiveStringMap, format: String): Option[StructType] = {
    val spark = SparkSession.active
    val logDir = options.get("logDir")
    if (logDir == null || logDir.isEmpty) return None

    val hadoopConf = spark.sparkContext.hadoopConfiguration
    options.asCaseSensitiveMap().asScala
      .filter(_._1.startsWith("hadoop."))
      .foreach { case (k, v) => hadoopConf.set(k.stripPrefix("hadoop."), v) }

    val logDirPath = new Path(logDir)
    val fs = logDirPath.getFileSystem(hadoopConf)
    if (!fs.exists(logDirPath)) return None

    val sampleFiles = collectSampleFiles(fs, logDirPath)
    if (sampleFiles.isEmpty) return None

    val params = options.asCaseSensitiveMap().asScala.toMap
    format match {
      case "json" =>
        val parsedOptions = new JSONOptionsInRead(
          CaseInsensitiveMap(params),
          spark.sessionState.conf.sessionLocalTimeZone,
          spark.sessionState.conf.columnNameOfCorruptRecord)
        JsonDataSource(parsedOptions).inferSchema(spark, sampleFiles, parsedOptions)
      case "csv" =>
        val parsedOptions = new CSVOptions(
          CaseInsensitiveMap(params),
          spark.sessionState.conf.csvColumnPruning,
          spark.sessionState.conf.sessionLocalTimeZone,
          spark.sessionState.conf.columnNameOfCorruptRecord)
        CSVDataSource(parsedOptions).inferSchema(spark, sampleFiles, parsedOptions)
      case _ => None
    }
  }

  private def collectSampleFiles(fs: FileSystem, logDirPath: Path): Seq[FileStatus] = {
    val files = new ArrayBuffer[FileStatus]()
    fs.listStatus(logDirPath).foreach { entry =>
      val name = entry.getPath.getName
      if (!name.startsWith(".") && !name.startsWith("_") && !name.endsWith(".inprogress")) {
        if (entry.isFile) {
          files += entry
        } else if (entry.isDirectory) {
          collectDirectoryFiles(fs, entry, files)
        }
      }
    }
    files
  }

  private def collectDirectoryFiles(
      fs: FileSystem,
      dirEntry: FileStatus,
      files: ArrayBuffer[FileStatus]): Unit = {
    val logFileFilter: Path => Boolean = { p =>
      val n = p.getName
      !n.startsWith(".") && !n.startsWith("_") && !n.endsWith(".compact")
    }

    fs.listStatus(dirEntry.getPath, (p: Path) => logFileFilter(p)).foreach { child =>
      if (child.isFile) {
        files += child
      } else if (child.isDirectory) {
        fs.listStatus(child.getPath, (p: Path) => logFileFilter(p)).foreach { nf =>
          if (nf.isFile) {
            files += nf
          }
        }
      }
    }
  }
}
