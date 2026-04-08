package cn.wangz.spark.connector.logfile

import java.io.{BufferedReader, InputStreamReader}
import java.nio.charset.StandardCharsets

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress.CompressionCodecFactory

class TextLogFilePartitionReader(
    filePath: String,
    appId: String,
    dt: String,
    hour: String,
    hadoopConf: Configuration)
  extends LogFilePartitionReader(filePath, appId, dt, hour, hadoopConf) {

  private lazy val textReader: BufferedReader = {
    val path = new Path(filePath)
    val fs = path.getFileSystem(hadoopConf)
    val raw = fs.open(path)
    val codecFactory = new CompressionCodecFactory(hadoopConf)
    val codec = codecFactory.getCodec(path)
    val in = if (codec != null) codec.createInputStream(raw) else raw
    new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))
  }

  override def next(): Boolean = {
    var line = textReader.readLine()
    while (line != null) {
      val trimmed = line.trim
      if (trimmed.nonEmpty) {
        currentLine = trimmed
        return true
      }
      line = textReader.readLine()
    }
    false
  }

  override def close(): Unit = {
    textReader.close()
  }
}
