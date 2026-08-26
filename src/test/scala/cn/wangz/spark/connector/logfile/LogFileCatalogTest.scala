package cn.wangz.spark.connector.logfile

import java.util

import org.apache.spark.sql.catalyst.analysis.{NoSuchNamespaceException, NoSuchTableException}
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalatest.funsuite.AnyFunSuite

class LogFileCatalogTest extends AnyFunSuite {

  private def newCatalog(): LogFileCatalog = {
    val options = new util.HashMap[String, String]()
    options.put("logDir", "/unused")
    options.put("fileFormat", "text")
    val catalog = new LogFileCatalog
    catalog.initialize("log_catalog", new CaseInsensitiveStringMap(options))
    catalog
  }

  test("list the table only in root and default namespaces") {
    val catalog = newCatalog()

    assert(catalog.listTables(Array.empty).map(_.name()).sameElements(Array("spark_log_file")))
    assert(catalog.listTables(Array("DEFAULT")).map(_.name())
      .sameElements(Array("spark_log_file")))
    intercept[NoSuchNamespaceException] {
      catalog.listTables(Array("other"))
    }
    intercept[NoSuchNamespaceException] {
      catalog.listTables(Array("default", "nested"))
    }
  }

  test("loadTable and tableExists enforce namespace and table name") {
    val catalog = newCatalog()
    val rootTable = Identifier.of(Array.empty, "SPARK_LOG_FILE")
    val defaultTable = Identifier.of(Array("default"), "spark_log_file")
    val otherNamespace = Identifier.of(Array("other"), "spark_log_file")
    val nestedNamespace = Identifier.of(Array("default", "nested"), "spark_log_file")
    val wrongTable = Identifier.of(Array("default"), "other_table")

    assert(catalog.loadTable(rootTable) != null)
    assert(catalog.loadTable(defaultTable) != null)
    intercept[NoSuchTableException](catalog.loadTable(otherNamespace))
    intercept[NoSuchTableException](catalog.loadTable(nestedNamespace))
    intercept[NoSuchTableException](catalog.loadTable(wrongTable))

    assert(catalog.tableExists(rootTable))
    assert(catalog.tableExists(defaultTable))
    assert(!catalog.tableExists(otherNamespace))
    assert(!catalog.tableExists(nestedNamespace))
    assert(!catalog.tableExists(wrongTable))
  }
}
