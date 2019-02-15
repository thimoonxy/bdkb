package com.simon.dfs

import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress.{CompressionCodec, SnappyCodec}
import org.apache.parquet.column.ParquetProperties
import org.apache.parquet.example.data.Group
import org.apache.parquet.example.data.simple.SimpleGroupFactory
import org.apache.parquet.hadoop.api.WriteSupport
import org.apache.parquet.hadoop.example.GroupWriteSupport
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.{ParquetFileWriter, ParquetWriter}
import org.apache.parquet.schema.MessageTypeParser

object WriteParquetGzFile extends App{
  val conf = new Configuration()
  conf.setBoolean("dfs.support.append", true)
  conf.setBoolean("mapred.compress.map.output", true)
  conf.setBoolean("mapred.output.compress", true)
  conf.setClass("mapred.output.compression.codec",classOf[SnappyCodec],classOf[CompressionCodec])


  val path = new Path("misc/test3.snappy.parquet")
  val writeSupport:WriteSupport[Group] = new GroupWriteSupport()
  val schema = MessageTypeParser.parseMessageType("message Pair {\n" +
            " required binary city (UTF8);\n" +
            " required binary ip (UTF8);}")
  conf.set("parquet.example.schema", schema.toString())
  val factory = new SimpleGroupFactory(schema)
  val group = factory.newGroup().append("city","Beijing")
  .append("ip","192.168.1.1")
  val writer = new ParquetWriter(
    path, ParquetFileWriter.Mode.OVERWRITE, writeSupport, CompressionCodecName.SNAPPY,
    128*1024*1024, 5*1024*1024, 5*1024*1024,
    true, false,
    ParquetProperties.WriterVersion.PARQUET_1_0,
    conf)
  var count = 2l<<80
  while(count>=0){
    writer.write(group)
    count-=1
  }
  IOUtils.closeQuietly(writer)
}
