package cn.wangz.spark.connector.logfile

import java.io.File
import java.nio.charset.StandardCharsets

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.file.tfile.TFile

object TFileTestDataGenerator {

  def main(args: Array[String]): Unit = {
    val outputDir = new File("src/test/resources/tfile_logs")
    outputDir.mkdirs()
    generate(
      new File(outputDir, "app_tfile_001").getAbsolutePath,
      Seq("tfile event 1", "tfile event 2", "tfile event 3"))
    println("TFile test resources generated successfully.")
  }

  def generate(path: String, entries: Seq[String]): Unit = {
    val conf = new Configuration()
    val hadoopPath = new Path(path)
    val fs = hadoopPath.getFileSystem(conf)
    val out = fs.create(hadoopPath, true)
    val writer = new TFile.Writer(out, 64 * 1024, "none", null, conf)
    try {
      entries.foreach { entry =>
        writer.append(Array.emptyByteArray, entry.getBytes(StandardCharsets.UTF_8))
      }
    } finally {
      writer.close()
      out.close()
    }
  }
}
