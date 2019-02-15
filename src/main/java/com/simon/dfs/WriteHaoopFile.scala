package com.simon.dfs

object WriteHaoopFile extends App{
    import org.apache.hadoop.conf.Configuration
    import org.apache.hadoop.fs.{FileSystem, Path}
    val conf = new Configuration()
    conf.setBoolean("dfs.support.append", true)
    val fs = FileSystem.get(conf)
    val fh = fs.create(new Path("misc/testOut.txt"))
    val j = "beijing  192.188.1.1"
    fh.writeBytes(j)
      var count = 2l<<80
      while(count>=0){
        fh.writeBytes(j)
        count-=1
      }
    fh.close()
}
