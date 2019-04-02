package com.simon.concurrent

import java.time._

import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success}

/*
task1 started@ 11:50:14.141
task4 started@ 11:50:14.141
task7 started@ 11:50:14.141
task0 started@ 11:50:14.141
task6 started@ 11:50:14.141
task5 started@ 11:50:14.141
task2 started@ 11:50:14.141
task3 started@ 11:50:14.141
task0 Done@ 11:50:14.144
task8 started@ 11:50:14.144
task1 Done@ 11:50:15.147
task9 started@ 11:50:15.147
task2 Done@ 11:50:16.148
task3 Done@ 11:50:17.147
task4 Done@ 11:50:18.149
task5 Done@ 11:50:19.145
timeout
*/
object ManyFutures extends App{

  val timeout = 5 // seconds
  var slept = 0 // millis
  val nap = 100 // millis
  def allCompleted(tasks:ArrayBuffer[Future[Int]]):Boolean={
    var res = true
    tasks.foreach(f=>{res = res && f.isCompleted})
    res
  }

  val taskArr = new ArrayBuffer[Future[Int]]
  for (x <- 0 until 10) {  // cpu has 4*dual = 8 processers
    taskArr.append(DemoFuture.genFuture(s"task${x}", math.abs(x), x))
  }

  while (!allCompleted(taskArr) && slept  < timeout * 1000) {
    Thread.sleep(nap)
    slept += nap
  }

  if(slept  >= timeout * 1000) {
    println("timeout")
  } else{
    println("all done.")
  }

}
