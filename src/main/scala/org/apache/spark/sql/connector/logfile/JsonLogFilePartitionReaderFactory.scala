package org.apache.spark.sql.connector.logfile

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.json.JSONOptionsInRead
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.datasources.v2.FilePartitionReaderFactory
import org.apache.spark.sql.execution.datasources.v2.json.JsonPartitionReaderFactory
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

object JsonLogFilePartitionReaderFactory {
  def apply(
      sqlConf: SQLConf,
      broadcastedConf: Broadcast[SerializableConfiguration],
      dataSchema: StructType,
      readDataSchema: StructType,
      partitionSchema: StructType,
      parameters: Map[String, String],
      filters: Seq[Filter] = Seq.empty): JsonLogFilePartitionReaderFactory = {
    val options = new JSONOptionsInRead(
      CaseInsensitiveMap(parameters),
      sqlConf.sessionLocalTimeZone,
      sqlConf.columnNameOfCorruptRecord)
    new JsonLogFilePartitionReaderFactory(
      sqlConf, broadcastedConf, dataSchema, readDataSchema, partitionSchema, options, filters)
  }
}

class JsonLogFilePartitionReaderFactory(
    sqlConf: SQLConf,
    broadcastedConf: Broadcast[SerializableConfiguration],
    dataSchema: StructType,
    readDataSchema: StructType,
    partitionSchema: StructType,
    options: JSONOptionsInRead,
    filters: Seq[Filter]) extends DelegateLogFilePartitionReaderFactory {

  override protected val delegate: FilePartitionReaderFactory = JsonPartitionReaderFactory(
    sqlConf, broadcastedConf, dataSchema, readDataSchema, partitionSchema, options, filters)
}
