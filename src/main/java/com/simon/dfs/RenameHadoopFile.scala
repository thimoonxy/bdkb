package com.simon.dfs

import java.io.FileNotFoundException

import org.apache.hadoop.fs.permission.FsPermission

object RenameHadoopFile extends App{
    import org.apache.hadoop.conf.Configuration
    import org.apache.hadoop.fs.{FileSystem, Path}
    val hconf = new Configuration()
    hconf.setBoolean("dfs.support.append", true)
    val fs = FileSystem.get(hconf)
    if(fs.delete(new Path("dstFolder"),true)) println("deleted")
    if(fs.mkdirs(new Path("testFolder"),FsPermission.getCachePoolDefault())) println("created")
    if(fs.rename(new Path("testFolder"),new Path("dstFolder"))) println("renamed")
    println(fs.getFileStatus(new Path("dstFolder")).getModificationTime)
    try {
      println(fs.getFileStatus(new Path("progressing")).isDirectory)
    } catch{
      case e: FileNotFoundException=>{
        println("not found")
      }
    }

    fs.close()
}
