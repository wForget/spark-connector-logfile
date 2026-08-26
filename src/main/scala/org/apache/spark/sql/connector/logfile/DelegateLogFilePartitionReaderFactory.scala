package org.apache.spark.sql.connector.logfile

import cn.wangz.spark.connector.logfile.LogFilePartition

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.execution.datasources.{FilePartition, PartitionedFile}
import org.apache.spark.sql.execution.datasources.v2.FilePartitionReaderFactory
import org.apache.spark.paths.SparkPath
import org.apache.spark.unsafe.types.UTF8String

abstract class DelegateLogFilePartitionReaderFactory extends PartitionReaderFactory {

  protected def delegate: FilePartitionReaderFactory

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val p = partition.asInstanceOf[LogFilePartition]
    val partitionValues = new GenericInternalRow(Array[Any](
      UTF8String.fromString(p.dt),
      UTF8String.fromString(p.hour),
      UTF8String.fromString(p.appId)
    ))
    val length = if (p.fileSize >= 0) p.fileSize else Long.MaxValue
    val partitionedFile = PartitionedFile(
      partitionValues, SparkPath.fromPathString(p.filePath), 0, length)
    delegate.createReader(FilePartition(0, Array(partitionedFile)))
  }
}
