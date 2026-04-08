package cn.wangz.spark.connector.logfile

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.unsafe.types.UTF8String

abstract class LogFilePartitionReader(
    filePath: String,
    appId: String,
    dt: String,
    hour: String,
    hadoopConf: Configuration) extends PartitionReader[InternalRow] {

  private val appIdUtf8 = UTF8String.fromString(appId)
  private val dtUtf8 = UTF8String.fromString(dt)
  private val hourUtf8 = UTF8String.fromString(hour)

  protected var currentLine: String = _

  override def get(): InternalRow = {
    new GenericInternalRow(Array[Any](
      UTF8String.fromString(currentLine),
      dtUtf8,
      hourUtf8,
      appIdUtf8
    ))
  }
}
