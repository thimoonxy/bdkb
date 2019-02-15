package com.simon.dfs

import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.parquet.column.ParquetProperties
import org.apache.parquet.example.data.simple.SimpleGroupFactory
import org.apache.parquet.hadoop.example.GroupWriteSupport
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.{ParquetFileWriter, ParquetWriter}
import org.apache.parquet.schema.MessageTypeParser

object WriteParuqetFile extends App{

  val conf = new Configuration()
  conf.setBoolean("mapred.compress.map.output", false)
  conf.setBoolean("mapred.output.compress", false)
  val fs = FileSystem.get(conf)
  val path = new Path("misc/test2.parquet")
  val writeSupport = new GroupWriteSupport()

  val schema = MessageTypeParser.parseMessageType("message Pair {\n" +
            " required binary city (UTF8);\n" +
            " required binary ip (UTF8);}")
  conf.set("parquet.example.schema", schema.toString())

  val factory = new SimpleGroupFactory(schema)
  val group = factory.newGroup()
    .append("city","Beijing")
    .append("ip","192.168.1.1")
//  val writer =  new ParquetWriter(path, conf, new GroupWriteSupport)

  val writer = new ParquetWriter(
    path, ParquetFileWriter.Mode.OVERWRITE, writeSupport, CompressionCodecName.UNCOMPRESSED,
    4*1024, 5*1024*1024, 5*1024*1024,
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
