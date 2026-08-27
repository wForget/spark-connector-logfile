package org.apache.spark.sql.connector.logfile

import java.io.OutputStream
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

import org.apache.spark.SparkConf
import org.apache.spark.io.CompressionCodec

object SparkEventLogTestDataGenerator {

  def write(path: Path, codecName: String, records: Seq[String]): Unit = {
    val fileStream = Files.newOutputStream(path)
    var compressedStream: OutputStream = null
    try {
      val codec = CompressionCodec.createCodec(new SparkConf(false), codecName)
      compressedStream = codec.compressedContinuousOutputStream(fileStream)
      records.foreach { record =>
        compressedStream.write((record + "\n").getBytes(StandardCharsets.UTF_8))
        compressedStream.flush()
      }
    } finally {
      if (compressedStream != null) {
        compressedStream.close()
      } else {
        fileStream.close()
      }
    }
    path.toFile.deleteOnExit()
  }
}
