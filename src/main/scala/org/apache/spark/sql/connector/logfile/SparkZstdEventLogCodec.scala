package org.apache.spark.sql.connector.logfile

import java.io.{InputStream, OutputStream}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.compress.{
  CompressionCodec => HadoopCompressionCodec,
  CompressionInputStream,
  CompressionOutputStream,
  Compressor,
  Decompressor
}
import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.io.{CompressionCodec => SparkCompressionCodec}

/**
 * Exposes Spark's Zstd stream as a Hadoop codec so Spark 3.5's line-based file readers can
 * recognize the `.zstd` extension used by Spark event logs.
 *
 * Spark event logs may contain multiple Zstd frames because the event logger closes the current
 * frame on flush. The continuous input stream is therefore required instead of Hadoop's `.zst`
 * codec or Spark's regular compressed input stream.
 */
class SparkZstdEventLogCodec extends HadoopCompressionCodec {

  override def createInputStream(input: InputStream): CompressionInputStream = {
    val sparkConf = Option(SparkEnv.get).map(_.conf).getOrElse(new SparkConf(false))
    val decompressed = SparkCompressionCodec.createCodec(sparkConf, "zstd")
      .compressedContinuousInputStream(input)
    new SparkZstdEventLogInputStream(input, decompressed)
  }

  override def createInputStream(
      input: InputStream,
      decompressor: Decompressor): CompressionInputStream = createInputStream(input)

  override def getDefaultExtension: String = ".zstd"

  override def createOutputStream(output: OutputStream): CompressionOutputStream =
    throw new UnsupportedOperationException("SparkZstdEventLogCodec is read-only")

  override def createOutputStream(
      output: OutputStream,
      compressor: Compressor): CompressionOutputStream = createOutputStream(output)

  override def getCompressorType: Class[_ <: Compressor] = null

  override def createCompressor(): Compressor = null

  override def getDecompressorType: Class[_ <: Decompressor] = null

  override def createDecompressor(): Decompressor = null
}

object SparkZstdEventLogCodec {
  private val HadoopCodecsKey = "io.compression.codecs"
  private val CodecClassName = classOf[SparkZstdEventLogCodec].getName

  def register(conf: Configuration): Unit = {
    val configured = Option(conf.get(HadoopCodecsKey)).toSeq
      .flatMap(_.split(','))
      .map(_.trim)
      .filter(_.nonEmpty)
    if (!configured.contains(CodecClassName)) {
      conf.set(HadoopCodecsKey, (configured :+ CodecClassName).mkString(","))
    }
  }

  def configuredCodecs(conf: Configuration): String = conf.get(HadoopCodecsKey)
}

private class SparkZstdEventLogInputStream(
    compressed: InputStream,
    decompressed: InputStream) extends CompressionInputStream(compressed) {

  override def read(): Int = decompressed.read()

  override def read(buffer: Array[Byte], offset: Int, length: Int): Int =
    decompressed.read(buffer, offset, length)

  override def skip(length: Long): Long = decompressed.skip(length)

  override def available(): Int = decompressed.available()

  override def close(): Unit = decompressed.close()

  override def resetState(): Unit = ()
}
