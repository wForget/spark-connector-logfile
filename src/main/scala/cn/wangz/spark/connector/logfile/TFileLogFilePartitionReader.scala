package cn.wangz.spark.connector.logfile

import java.nio.charset.StandardCharsets

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, Path}
import org.apache.hadoop.io.file.tfile.TFile

class TFileLogFilePartitionReader(
    filePath: String,
    appId: String,
    dt: String,
    hour: String,
    hadoopConf: Configuration)
  extends LogFilePartitionReader(filePath, appId, dt, hour, hadoopConf) {

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

  override def close(): Unit = {
    scanner.close()
    reader.close()
    fsdis.close()
  }
}
