package cn.wangz.spark.connector.logfile

import org.apache.hadoop.conf.Configuration
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.util.SerializableConfiguration

class TFileLogFilePartitionReaderFactory(
    broadcastedConf: Broadcast[SerializableConfiguration]) extends PartitionReaderFactory {

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val p = partition.asInstanceOf[LogFilePartition]
    val conf = new Configuration(broadcastedConf.value.value)
    new TFileLogFilePartitionReader(p.filePath, p.appId, p.dt, p.hour, conf)
  }
}
