package com.simon.hbase

import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes

object DemoV1 {
  val tableName = "test_table"
  val columnFamily= "score"
  val columnQualifier = "math"
  val row = "Simon"
  val rb = Bytes.toBytes(row)
  val cf = Bytes.toBytes(s"${this.columnFamily}")
  val cq = Bytes.toBytes(this.columnQualifier)

  def main(args: Array[String]): Unit = {

    //Connection
    val hbaseConf = HBaseConfiguration.create()
        hbaseConf.set("hbase.zookeeper.property.clientPort", "2182")
        hbaseConf.set("hbase.zookeeper.quorum", "myhbase")
    val connect = ConnectionFactory.createConnection(hbaseConf)
    val admin = connect.getAdmin

    // Get
    /*
        hbase(main):006:0> get 'test_table', 'Simon' ,{COLUMN => ['score:math:toInt'], TIMESTAMP => 1554376708704}
        COLUMN                            CELL
         score:math                       timestamp=1554376708704, value=95
        1 row(s) in 0.0110 seconds

        hbase(main):023:0> get 'test_table' ,'Simon', 'score:math:toInt'
        COLUMN                            CELL
         score:math                       timestamp=1554706728104, value=95
        1 row(s) in 0.0080 seconds
     */
    val get = (stage:String, table:Table)=>{
      println(s"\n#### ${stage}")
      try{
        val g = new Get(rb)
        val r:Result = table.get(g)
        val rowKey:String = Bytes.toString(r.getRow())
        println("  rowKey: "+rowKey)  // Simon
        val vb = r.getValue(cf, cq)
        val columnValue = Bytes.toInt(vb)  // toInt , not toString, in case unwanted Casting
        println("  value: "+columnValue)  // 95
      } catch {
        case _:Exception => println("[Get] not found")
      }

    }

    //Create
    val tableName:TableName= TableName.valueOf(this.tableName)
    if (admin.listTableNames().contains(tableName)) {
      println("Table already found")
    } else {
      val family:HColumnDescriptor = new HColumnDescriptor(cf)
      val desc:HTableDescriptor = new HTableDescriptor(tableName)
      desc.addFamily(family)
      admin.createTable(desc)
      println(s"Table ${this.tableName} created")
    }


    // Insert
    val table = connect.getTable(tableName)
    val p = new Put(rb)
    p.addColumn(cf, cq, Bytes.toBytes(95))
    table.put(p)
    get("insert",table)

    // update
    val u = new Put(rb)
    u.addColumn(cf, cq, Bytes.toBytes(89))
    table.put(u)
    get("update",table)

    // delete value
    val d = new Delete(rb)
        d.addColumns(cf, cq)
    // NOTE:
    // - use d.addColumnS() to remove all versions
    // - use d.addColumn() to remove the latest version
    table.delete(d)
    get("delete",table)
    table.close()

    // Delete table
    admin.disableTable(tableName)
    println("disable table")
    if (admin.isTableDisabled(tableName)) {
      admin.deleteTable(tableName)
      println(s"Table ${this.tableName} deleted")
    } else {
      println("disable failed")
    }

    admin.close()
    connect.close()

  }
}
