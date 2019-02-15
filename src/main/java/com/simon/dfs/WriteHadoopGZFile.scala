package com.simon.dfs

object WriteHadoopGZFile {
// 行数到纯文本测试
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.compress.{CompressionCodec, CompressionOutputStream}
import org.apache.hadoop.util.ReflectionUtils
    val conf = new Configuration()
    var out: CompressionOutputStream = null
    try {
      val codec = ReflectionUtils.newInstance(Class.forName("org.apache.hadoop.io.compress.GzipCodec"), conf).asInstanceOf[CompressionCodec]
      val fs = FileSystem.get(conf)
      var lastLineCid = "-"

      out = codec.createOutputStream(fs.create(new Path("misc/testOut.gz"), true))
      for(_<- 0 until 1000000)
      out.write(("""117.136.86.184 - - [22/Aug/2018:03:00:31 +0800] "GET http://ax.init.itunes.apple.com/bag.xml?os=6&ix=2&locale=zh_CN HTTP/1.1" 200 195367 "-" "itsMetricsR=Search-CN@@Media%20Search%20Pages@@@@; xt-b-ts-800148225=1450267443863; xt-src=b; xp_ci=3z4Tsy18zHR0z4XJz994z9pGvo7NC;" 2""" + "\n").getBytes)
      out.close()
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
        ("file save exception")
    } finally {
      if (null != out) {
        try {
          out.close()
        } catch {
          case ex: Exception =>
            ex.printStackTrace()
            println("close exception")
        }
      }
    }
}
