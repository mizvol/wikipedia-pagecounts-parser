/**
  * Created by volodymyrmiz on 16/08/18.
  */

import java.util.Calendar

import ch.epfl.lts2.Utils.suppressLogs
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.array

object Tests extends App {

  suppressLogs(List("org", "akka"))

  val spark = SparkSession.builder
    .master("local[*]")
    .appName("Spark Word Count")
    .getOrCreate()

//  val lines = spark.sparkContext.parallelize(
//    Seq("Spark Intellij Idea Scala test one",
//      "Spark Intellij Idea Scala test two",
//      "Spark Intellij Idea Scala test three"))
//
//  val counts = lines
//    .flatMap(line => line.split(" "))
//    .map(word => (word, 1))
//    .reduceByKey(_ + _)
//
//  counts.foreach(println)

//  import spark.implicits._
//  val left = Seq((0, "zero"), (1, "one")).toDF("page", "dailyTotal")
//  val right = Seq((0, "zero"), (2, "two"), (3, "three")).toDF("page", "dailyTotal")
//
//  val df = right.withColumnRenamed("dailyTotal", "dt1").join(left.withColumnRenamed("dailyTotal", "dt2"), Seq("page"), "fullouter").select($"*", array($"dt1", $"dt2"))
//
//  df.show()

  println(Calendar.getInstance().getTime())


}