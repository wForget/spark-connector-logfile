package cn.wangz.spark.connector.logfile

import java.nio.charset.StandardCharsets

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.file.tfile.TFile
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.unsafe.types.UTF8String

class TFileLogFilePartitionReader(
    filePath: String,
    appId: String,
    dt: String,
    hour: String,
    hadoopConf: Configuration) extends PartitionReader[InternalRow] {

  private val appIdUtf8 = UTF8String.fromString(appId)
  private val dtUtf8 = UTF8String.fromString(dt)
  private val hourUtf8 = UTF8String.fromString(hour)

  private var currentLine: String = _

  private lazy val (fsdis, reader, scanner) = {
    val path = new Path(filePath)
    val fs = path.getFileSystem(hadoopConf)
    val fsdis = fs.open(path)
    val len = fs.getFileStatus(path).getLen
    val reader = new TFile.Reader(fsdis, len, hadoopConf)
    val scanner = reader.createScanner()
    (fsdis, reader, scanner)
  }

  override def next(): Boolean = {
    while (!scanner.atEnd()) {
      val entry = scanner.entry()
      val valBytes = new Array[Byte](entry.getValueLength)
      entry.getValueStream.readFully(valBytes)
      scanner.advance()

      val trimmed = new String(valBytes, StandardCharsets.UTF_8).trim
      if (trimmed.nonEmpty) {
        currentLine = trimmed
        return true
      }
    }
    false
  }

  override def get(): InternalRow = {
    new GenericInternalRow(Array[Any](
      UTF8String.fromString(currentLine),
      dtUtf8,
      hourUtf8,
      appIdUtf8
    ))
  }

  override def close(): Unit = {
    try {
      scanner.close()
    } finally {
      try {
        reader.close()
      } finally {
        fsdis.close()
      }
    }
  }
}
