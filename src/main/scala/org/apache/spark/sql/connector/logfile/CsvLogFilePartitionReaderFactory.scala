package org.apache.spark.sql.connector.logfile

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.csv.CSVOptions
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.datasources.v2.FilePartitionReaderFactory
import org.apache.spark.sql.execution.datasources.v2.csv.CSVPartitionReaderFactory
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

object CsvLogFilePartitionReaderFactory {
  def apply(
      sqlConf: SQLConf,
      broadcastedConf: Broadcast[SerializableConfiguration],
      dataSchema: StructType,
      readDataSchema: StructType,
      partitionSchema: StructType,
      parameters: Map[String, String],
      filters: Seq[Filter] = Seq.empty): CsvLogFilePartitionReaderFactory = {
    val options = new CSVOptions(
      CaseInsensitiveMap(parameters),
      sqlConf.csvColumnPruning,
      sqlConf.sessionLocalTimeZone,
      sqlConf.columnNameOfCorruptRecord)
    new CsvLogFilePartitionReaderFactory(
      sqlConf, broadcastedConf, dataSchema, readDataSchema, partitionSchema, options, filters)
  }
}

class CsvLogFilePartitionReaderFactory(
    sqlConf: SQLConf,
    broadcastedConf: Broadcast[SerializableConfiguration],
    dataSchema: StructType,
    readDataSchema: StructType,
    partitionSchema: StructType,
    options: CSVOptions,
    filters: Seq[Filter]) extends DelegateLogFilePartitionReaderFactory {

  override protected val delegate: FilePartitionReaderFactory = CSVPartitionReaderFactory(
    sqlConf, broadcastedConf, dataSchema, readDataSchema, partitionSchema, options, filters)
}
