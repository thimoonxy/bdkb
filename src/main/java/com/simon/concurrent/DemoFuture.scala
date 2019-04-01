package com.simon.concurrent
import java.time._

import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

/**
  * Begin @ 16:46:19.108
  * task2 started@ 16:46:19.156
  * task1 started@ 16:46:19.156
  * task2 Done@ 16:46:49.159
  * task1 Done@ 16:47:19.158
  * f1+f2=5 @ 16:47:19.164
  * Finish @ 16:47:19.174
  */
object DemoFuture extends App {
  def genFuture(taskName:String,taskDuration:Int, value:Int) :Future[Int] ={
    Future[Int] {
      println(s"${taskName} started@ ${LocalTime.now()}")
      Thread.sleep(1000 * taskDuration  )  // Task duration, e.g. downloading stuffs from website
      println(s"${taskName} Done@ ${LocalTime.now()}")
      value    // value returned
    }
  }

  println(s"Begin @ ${LocalTime.now()}")
  val f1:Future[Int] = genFuture("task1",60, 2)
  val f2:Future[Int] = genFuture("task2",30, 3)
  val combined:Future[Int] = f1.flatMap( x=> f2.map(y=> x +y))
  combined onComplete {
    case Success(v) => println(s"f1+f2=${v} @ ${LocalTime.now()}")
    case Failure(ex) => println(ex.getMessage)
  }
  while (!combined.isCompleted) Thread.sleep(10)
  println(s"Finish @ ${LocalTime.now()}")
}
