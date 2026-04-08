package cn.wangz.spark.connector.logfile

import org.apache.spark.sql.connector.read.InputPartition

case class LogFilePartition(
    filePath: String,
    appId: String,
    dt: String,
    hour: String,
    fileSize: Long = -1L) extends InputPartition
