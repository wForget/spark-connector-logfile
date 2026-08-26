package cn.wangz.spark.connector.logfile

import java.util

import org.apache.spark.sql.catalyst.analysis.{NoSuchNamespaceException, NoSuchTableException}
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.catalyst.InternalRow
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

  test("reject scan options that redefine table schema or location") {
    val catalogOptions = new util.HashMap[String, String]()
    catalogOptions.put("logDir", "/catalog/logs")
    catalogOptions.put("fileFormat", "text")
    catalogOptions.put("inferSchema", "false")
    val table = new LogFileTable(new CaseInsensitiveStringMap(catalogOptions))

    Seq(
      "logDir" -> "/scan/logs",
      "fileFormat" -> "json",
      "inferSchema" -> "true"
    ).foreach { case (key, value) =>
      val scanOptions = new util.HashMap[String, String]()
      scanOptions.put(key, value)
      val error = intercept[IllegalArgumentException] {
        table.newScanBuilder(new CaseInsensitiveStringMap(scanOptions))
      }
      assert(error.getMessage.contains(s"Scan option '$key' cannot override"))
    }
  }

  test("allow equivalent table-level scan options") {
    val catalogOptions = new util.HashMap[String, String]()
    catalogOptions.put("logDir", "/catalog/logs")
    catalogOptions.put("fileFormat", "text")
    catalogOptions.put("inferSchema", "false")
    val table = new LogFileTable(new CaseInsensitiveStringMap(catalogOptions))

    val scanOptions = new util.HashMap[String, String]()
    scanOptions.put("LOGDIR", "/catalog/logs")
    scanOptions.put("FILEFORMAT", "TEXT")
    scanOptions.put("INFERSCHEMA", "FALSE")
    assert(table.newScanBuilder(new CaseInsensitiveStringMap(scanOptions)) != null)
  }

  test("allow an equivalent scan schema and reject a schema change") {
    val catalogOptions = new util.HashMap[String, String]()
    catalogOptions.put("logDir", "/catalog/logs")
    catalogOptions.put("fileFormat", "json")
    catalogOptions.put("schema", "value STRING, level STRING")
    val table = new LogFileTable(new CaseInsensitiveStringMap(catalogOptions))

    val equivalent = new util.HashMap[String, String]()
    equivalent.put("SCHEMA", "`value` STRING, `level` STRING")
    assert(table.newScanBuilder(new CaseInsensitiveStringMap(equivalent)) != null)

    val changed = new util.HashMap[String, String]()
    changed.put("schema", "value STRING, level INT")
    val error = intercept[IllegalArgumentException] {
      table.newScanBuilder(new CaseInsensitiveStringMap(changed))
    }
    assert(error.getMessage.contains("Scan option 'schema' cannot override"))
  }

  test("reject an explicit schema for value-only file formats") {
    val catalogOptions = new util.HashMap[String, String]()
    catalogOptions.put("logDir", "/catalog/logs")
    catalogOptions.put("fileFormat", "text")
    catalogOptions.put("schema", "value STRING")
    val table = new LogFileTable(new CaseInsensitiveStringMap(catalogOptions))

    val error = intercept[IllegalArgumentException](table.schema())
    assert(error.getMessage.contains("only supported for json and csv"))
  }

  test("validate partition identifier field names and arity") {
    val options = new util.HashMap[String, String]()
    options.put("logDir", "/does-not-exist")
    options.put("fileFormat", "text")
    val table = new LogFileTable(new CaseInsensitiveStringMap(options))

    intercept[IllegalArgumentException] {
      table.listPartitionIdentifiers(Array("dt"), InternalRow.empty)
    }
    intercept[IllegalArgumentException] {
      table.listPartitionIdentifiers(Array("unknown"), InternalRow("value"))
    }
    intercept[IllegalArgumentException] {
      table.listPartitionIdentifiers(Array("dt", "DT"), InternalRow("a", "b"))
    }
  }
}
