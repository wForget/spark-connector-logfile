package cn.wangz.spark.connector.logfile

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}

class LogFilePartitionReaderFactory(
    fileFormat: String,
    hadoopOptions: Map[String, String]) extends PartitionReaderFactory {

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val p = partition.asInstanceOf[LogFilePartition]
    val conf = new Configuration()
    hadoopOptions.foreach { case (k, v) => conf.set(k, v) }
    if (fileFormat.equalsIgnoreCase("tfile")) {
      new TFileLogFilePartitionReader(p.filePath, p.appId, p.dt, p.hour, conf)
    } else {
      new TextLogFilePartitionReader(p.filePath, p.appId, p.dt, p.hour, conf)
    }
  }
}
