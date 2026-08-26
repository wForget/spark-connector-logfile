package cn.wangz.spark.connector.logfile

import org.apache.hadoop.conf.Configuration
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.FileSourceOptions
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.logfile.DelegateLogFilePartitionReaderFactory
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.datasources.v2.FilePartitionReaderFactory
import org.apache.spark.util.SerializableConfiguration

class TFileLogFilePartitionReaderFactory(
    broadcastedConf: Broadcast[SerializableConfiguration],
    parameters: Map[String, String]) extends DelegateLogFilePartitionReaderFactory {

  def this(broadcastedConf: Broadcast[SerializableConfiguration]) =
    this(broadcastedConf, Map.empty)

  override protected val delegate: FilePartitionReaderFactory = new FilePartitionReaderFactory {
    override protected val options: FileSourceOptions = new FileSourceOptions(parameters)

    override def buildReader(file: PartitionedFile): PartitionReader[InternalRow] = {
      val conf = new Configuration(broadcastedConf.value.value)
      val partitionValues = file.partitionValues
      new TFileLogFilePartitionReader(
        file.toPath.toString,
        partitionValues.getUTF8String(2).toString,
        partitionValues.getUTF8String(0).toString,
        partitionValues.getUTF8String(1).toString,
        conf)
    }
  }
}
