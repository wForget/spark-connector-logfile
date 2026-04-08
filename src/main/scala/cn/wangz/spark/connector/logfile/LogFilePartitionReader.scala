package cn.wangz.spark.connector.logfile

import java.io.{BufferedReader, InputStreamReader}
import java.nio.charset.StandardCharsets

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import org.apache.hadoop.io.compress.CompressionCodecFactory
import org.apache.hadoop.io.file.tfile.TFile
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.unsafe.types.UTF8String

class LogFilePartitionReader(
    filePath: String,
    appId: String,
    dt: String,
    hour: String,
    fileFormat: String,
    hadoopConf: Configuration) extends PartitionReader[InternalRow] {

  private val appIdUtf8 = UTF8String.fromString(appId)
  private val dtUtf8 = UTF8String.fromString(dt)
  private val hourUtf8 = UTF8String.fromString(hour)

  private lazy val (textReader, tfileState) = init()
  private var currentLine: String = _

  private case class TFileState(
      fsdis: FSDataInputStream,
      reader: TFile.Reader,
      scanner: TFile.Reader.Scanner)

  private def init(): (BufferedReader, TFileState) = {
    val path = new Path(filePath)
    val fs = path.getFileSystem(hadoopConf)

    if (fileFormat.equalsIgnoreCase("tfile")) {
      val fsdis = fs.open(path)
      val len = fs.getFileStatus(path).getLen
      val reader = new TFile.Reader(fsdis, len, hadoopConf)
      val scanner = reader.createScanner()
      (null, TFileState(fsdis, reader, scanner))
    } else {
      val raw = fs.open(path)
      val codecFactory = new CompressionCodecFactory(hadoopConf)
      val codec = codecFactory.getCodec(path)
      val in = if (codec != null) codec.createInputStream(raw) else raw
      val br = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))
      (br, null)
    }
  }

  override def next(): Boolean = {
    if (fileFormat.equalsIgnoreCase("tfile")) nextTFile() else nextText()
  }

  private def nextText(): Boolean = {
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

  private def nextTFile(): Boolean = {
    val scanner = tfileState.scanner
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
    if (textReader != null) textReader.close()
    if (tfileState != null) {
      tfileState.scanner.close()
      tfileState.reader.close()
      tfileState.fsdis.close()
    }
  }
}
