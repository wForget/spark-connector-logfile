package cn.wangz.spark.connector.logfile

import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownFilters}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class LogFileScanBuilder(options: CaseInsensitiveStringMap, dataSchema: StructType)
    extends ScanBuilder with SupportsPushDownFilters {

  private var _pushedFilters: Array[Filter] = Array.empty

  override def build(): Scan = new LogFileScan(options, dataSchema, _pushedFilters)

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    val (pushed, remaining) = filters.partition(isPartitionFilter)
    _pushedFilters = pushed
    remaining
  }

  override def pushedFilters(): Array[Filter] = _pushedFilters

  private def isPartitionFilter(filter: Filter): Boolean = {
    val attr = filter match {
      case f: EqualTo            => Some(f.attribute)
      case f: In                 => Some(f.attribute)
      case f: StringStartsWith   => Some(f.attribute)
      case f: GreaterThan        => Some(f.attribute)
      case f: GreaterThanOrEqual => Some(f.attribute)
      case f: LessThan           => Some(f.attribute)
      case f: LessThanOrEqual    => Some(f.attribute)
      case _                     => None
    }
    attr.exists(a => LogFileTable.PARTITION_COLUMNS.contains(a.toLowerCase))
  }
}
