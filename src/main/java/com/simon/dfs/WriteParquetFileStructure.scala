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

object WriteParquetFileStructure extends App{
    val conf = new Configuration()
    conf.setBoolean("dfs.support.append", true)
    val fs = FileSystem.get(conf)
    val path = new Path("misc/test.parquet")
    val writeSupport = new GroupWriteSupport()

  val schema = MessageTypeParser.parseMessageType("message Pair {\n" +
                " required binary city (UTF8);\n" +
                " required binary ip (UTF8);\n" +
                " repeated group time {\n" +
                " required int32 ttl;\n"+
                " required binary ttl2;\n"+
                "}\n"+ "}")
  conf.set("parquet.example.schema", schema.toString())


  val factory = new SimpleGroupFactory(schema)
val group = factory.newGroup().append("city","Beijing")
  .append("ip","192.168.1.1")
  val tmpG = group.addGroup("time")
  tmpG.append("ttl",3)
  tmpG.append("ttl2","str2")
//    val writer =  new ParquetWriter[Group](path, conf, new GroupWriteSupport)
  val writer = new ParquetWriter(
    path, ParquetFileWriter.Mode.OVERWRITE, writeSupport, CompressionCodecName.GZIP,
    134217728, 1048576, 1048576,
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
