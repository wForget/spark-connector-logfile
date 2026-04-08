package org.apache.spark.sql.connector.logfile

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.datasources.text.TextOptions
import org.apache.spark.sql.execution.datasources.v2.FilePartitionReaderFactory
import org.apache.spark.sql.execution.datasources.v2.text.TextPartitionReaderFactory
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

object TextLogFilePartitionReaderFactory {
  def apply(
      sqlConf: SQLConf,
      broadcastedConf: Broadcast[SerializableConfiguration],
      readDataSchema: StructType,
      partitionSchema: StructType,
      parameters: Map[String, String]): TextLogFilePartitionReaderFactory = {
    val options = new TextOptions(CaseInsensitiveMap(parameters))
    new TextLogFilePartitionReaderFactory(
      sqlConf, broadcastedConf, readDataSchema, partitionSchema, options)
  }
}

class TextLogFilePartitionReaderFactory(
    sqlConf: SQLConf,
    broadcastedConf: Broadcast[SerializableConfiguration],
    readDataSchema: StructType,
    partitionSchema: StructType,
    options: TextOptions) extends DelegateLogFilePartitionReaderFactory {

  override protected val delegate: FilePartitionReaderFactory = TextPartitionReaderFactory(
    sqlConf, broadcastedConf, readDataSchema, partitionSchema, options)
}
