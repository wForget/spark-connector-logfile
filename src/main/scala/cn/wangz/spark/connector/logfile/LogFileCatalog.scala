package cn.wangz.spark.connector.logfile

import java.util

import scala.collection.JavaConverters._

import org.apache.spark.sql.SparkSession
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
    if (isSupportedNamespace(namespace)) {
      Array(Identifier.of(namespace, tableName))
    } else {
      throw new NoSuchNamespaceException(namespace)
    }
  }

  override def loadTable(ident: Identifier): Table = {
    if (isSupportedNamespace(ident.namespace()) && tableName.equalsIgnoreCase(ident.name())) {
      new LogFileTable(runtimeCatalogOptions())
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

  private def isSupportedNamespace(namespace: Array[String]): Boolean =
    namespace.isEmpty ||
      (namespace.length == 1 && namespace(0).equalsIgnoreCase("default"))

  private def runtimeCatalogOptions(): CaseInsensitiveStringMap = {
    SparkSession.getActiveSession.map { spark =>
      val prefix = s"spark.sql.catalog.$catalogName."
      val initializedSchemas = options.asCaseSensitiveMap().asScala
        .filter { case (key, _) => key.equalsIgnoreCase("schema") }
        .toSet
      val runtimeSchemas = spark.conf.getAll.collect {
        case (key, value)
            if key.startsWith(prefix) &&
              key.substring(prefix.length).equalsIgnoreCase("schema") =>
          key.substring(prefix.length) -> value
      }.filterNot(initializedSchemas.contains).toSeq
      val runtimeSchemaValues = runtimeSchemas.map(_._2).distinct
      require(runtimeSchemaValues.size <= 1,
        s"Conflicting runtime schema values for catalog '$catalogName': " +
          runtimeSchemas.map { case (key, value) => s"$key=$value" }.sorted.mkString(", "))

      runtimeSchemaValues.headOption.map { runtimeSchema =>
        val currentOptions = new util.HashMap[String, String]()
        currentOptions.put("schema", runtimeSchema)
        LogFileTable.mergeOptions(
          options,
          new CaseInsensitiveStringMap(currentOptions))
      }.getOrElse(options)
    }.getOrElse(options)
  }
}

object LogFileCatalog {
  val DefaultTableName: String = "spark_log_file"
}
