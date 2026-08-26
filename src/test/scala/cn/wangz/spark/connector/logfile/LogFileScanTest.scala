package cn.wangz.spark.connector.logfile

import java.nio.charset.StandardCharsets
import java.nio.file.attribute.FileTime
import java.nio.file.{Files, Path => NioPath}
import java.time.{Instant, ZoneId}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FilterFileSystem, Path, PathFilter}
import org.apache.spark.sql.sources.{
  EqualTo,
  Filter,
  GreaterThan,
  In,
  LessThanOrEqual,
  StringStartsWith
}
import org.scalatest.funsuite.AnyFunSuite

class LogFileScanTest extends AnyFunSuite {

  private val Utc = ZoneId.of("UTC")

  test("list log files through filtered listStatus calls") {
    val root = Files.createTempDirectory("log-file-path-filter-")
    val nested = Files.createDirectories(root.resolve("app_nested/one/two"))
    write(root.resolve("app_flat"))
    write(nested.resolve("events_1"))
    write(root.resolve("_ignored"))

    val trackingFs = new TrackingFileSystem(FileSystem.getLocal(new Configuration()))
    val listed = LogFileScan.listLogFiles(trackingFs, new Path(root.toUri))

    assert(listed.map(_._2).toSet === Set("app_flat", "app_nested"))
    assert(trackingFs.filteredListStatusCalls >= 3)
  }

  test("apply app_id filters before returning files while preserving file and directory naming") {
    val root = Files.createTempDirectory("log-file-app-filter-")
    write(root.resolve("flat.gz"))
    write(Files.createDirectories(root.resolve("dir.gz/nested")).resolve("events_1"))

    val fs = FileSystem.getLocal(new Configuration())
    val rootPath = new Path(root.toUri)

    assert(appIds(fs, rootPath, EqualTo("app_id", "flat")) === Seq("flat"))
    assert(appIds(fs, rootPath, EqualTo("app_id", "dir.gz")) === Seq("dir.gz"))

    val trackingFs = new TrackingFileSystem(fs)
    assert(appIds(
      trackingFs, rootPath, StringStartsWith("app_id", "missing")) === Seq.empty)
    assert(trackingFs.filteredListStatusCalls === 1)
  }

  test("apply dt hour and app_id filters to leaf FileStatus values in listLogFiles") {
    val root = Files.createTempDirectory("log-file-leaf-filter-")
    val early = write(root.resolve("app_early"))
    val late = write(root.resolve("app_late"))
    val nested = write(Files.createDirectories(root.resolve("app_nested")).resolve("events_1"))

    Files.setLastModifiedTime(early, FileTime.from(Instant.parse("2025-01-02T03:00:00Z")))
    Files.setLastModifiedTime(late, FileTime.from(Instant.parse("2025-01-03T04:00:00Z")))
    Files.setLastModifiedTime(nested, FileTime.from(Instant.parse("2025-01-03T05:00:00Z")))
    Files.setLastModifiedTime(
      root.resolve("app_nested"), FileTime.from(Instant.parse("2030-06-07T08:00:00Z")))

    val filters = Array[Filter](
      EqualTo("dt", "2025-01-03"),
      In("hour", Array[Any]("04", "05")),
      StringStartsWith("app_id", "app_l"))
    val fs = FileSystem.getLocal(new Configuration())
    val listed = LogFileScan.listLogFiles(fs, new Path(root.toUri), filters, Utc)

    assert(listed.map(_._2) === Seq("app_late"))

    val rangeFilters = Array[Filter](
      GreaterThan("dt", "2025-01-02"),
      LessThanOrEqual("hour", "04"))
    assert(LogFileScan.listLogFiles(fs, new Path(root.toUri), rangeFilters, Utc)
      .map(_._2) === Seq("app_late"))

    val nestedLeafFilters = Array[Filter](
      EqualTo("app_id", "app_nested"),
      EqualTo("dt", "2025-01-03"))
    assert(LogFileScan.listLogFiles(fs, new Path(root.toUri), nestedLeafFilters, Utc)
      .map(_._2) === Seq("app_nested"))
  }

  private def appIds(fs: FileSystem, root: Path, filter: Filter): Seq[String] =
    LogFileScan.listLogFiles(fs, root, Array(filter), Utc).map(_._2)

  private def write(path: NioPath): NioPath = {
    Files.write(path, "log\n".getBytes(StandardCharsets.UTF_8))
    path.toFile.deleteOnExit()
    path
  }

  private final class TrackingFileSystem(delegate: FileSystem)
      extends FilterFileSystem(delegate) {
    var filteredListStatusCalls: Int = 0

    override def listStatus(path: Path, filter: PathFilter) = {
      filteredListStatusCalls += 1
      super.listStatus(path, filter)
    }
  }
}
