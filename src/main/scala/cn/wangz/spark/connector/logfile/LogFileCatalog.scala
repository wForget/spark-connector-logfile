package cn.wangz.spark.connector.logfile

import java.util

import org.apache.spark.sql.catalyst.analysis.{NoSuchNamespaceException, NoSuchTableException}
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class LogFileCatalog extends CatalogPlugin with TableCatalog {

  private var catalogName: String = _
  private var tableName: String = _
  private var options: CaseInsensitiveStringMap = _

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    this.catalogName = name
    this.tableName = options.getOrDefault("tableName", LogFileCatalog.DefaultTableName)
    this.options = options
  }

  override def name(): String = catalogName

  override def listTables(namespace: Array[String]): Array[Identifier] = {
    if (namespace.isEmpty || (namespace.length == 1 && namespace(0) == "default")) {
      Array(Identifier.of(namespace, tableName))
    } else {
      throw new NoSuchNamespaceException(namespace)
    }
  }

  override def loadTable(ident: Identifier): Table = {
    if (tableName.equalsIgnoreCase(ident.name())) {
      new LogFileTable(options)
    } else {
      throw new NoSuchTableException(ident)
    }
  }

  override def createTable(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: util.Map[String, String]): Table =
    throw new UnsupportedOperationException("LogFileCatalog is read-only")

  override def alterTable(ident: Identifier, changes: TableChange*): Table =
    throw new UnsupportedOperationException("LogFileCatalog is read-only")

  override def dropTable(ident: Identifier): Boolean =
    throw new UnsupportedOperationException("LogFileCatalog is read-only")

  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit =
    throw new UnsupportedOperationException("LogFileCatalog is read-only")
}

object LogFileCatalog {
  val DefaultTableName: String = "spark_log_file"
}
