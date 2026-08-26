package cn.wangz.spark.connector.logfile

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, Path}
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

  private var currentValue: UTF8String = _
  private var fsdis: FSDataInputStream = _
  private var reader: TFile.Reader = _
  private var scanner: TFile.Reader.Scanner = _
  private var initialized = false
  private var closed = false
  private var initializationFailure: Throwable = _

  override def next(): Boolean = {
    initialize()
    if (!scanner.atEnd()) {
      val entry = scanner.entry()
      val valBytes = new Array[Byte](entry.getValueLength)
      entry.getValueStream.readFully(valBytes)
      scanner.advance()
      currentValue = UTF8String.fromBytes(valBytes)
      true
    } else {
      false
    }
  }

  override def get(): InternalRow = {
    new GenericInternalRow(Array[Any](
      currentValue,
      dtUtf8,
      hourUtf8,
      appIdUtf8
    ))
  }

  override def close(): Unit = {
    if (!closed) {
      closed = true
      closeResources()
    }
  }

  private def initialize(): Unit = {
    if (closed) {
      throw new IllegalStateException("TFile reader is closed")
    }
    if (initializationFailure != null) {
      throw initializationFailure
    }
    if (!initialized) {
      try {
        val path = new Path(filePath)
        val fs = path.getFileSystem(hadoopConf)
        fsdis = fs.open(path)
        val len = fs.getFileStatus(path).getLen
        reader = new TFile.Reader(fsdis, len, hadoopConf)
        scanner = reader.createScanner()
        initialized = true
      } catch {
        case failure: Throwable =>
          initializationFailure = failure
          closeResources(failure)
          throw failure
      }
    }
  }

  private def closeResources(primaryFailure: Throwable = null): Unit = {
    var failure = primaryFailure

    def close(resource: AnyRef)(closeAction: => Unit): Unit = {
      if (resource != null) {
        try {
          closeAction
        } catch {
          case closeFailure: Throwable =>
            if (failure == null) {
              failure = closeFailure
            } else if (failure ne closeFailure) {
              failure.addSuppressed(closeFailure)
            }
        }
      }
    }

    close(scanner)(scanner.close())
    scanner = null
    close(reader)(reader.close())
    reader = null
    close(fsdis)(fsdis.close())
    fsdis = null
    initialized = false

    if (primaryFailure == null && failure != null) throw failure
  }
}
