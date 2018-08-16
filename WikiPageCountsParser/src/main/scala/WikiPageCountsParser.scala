/**
  * Created by volodymyrmiz on 16/08/18.
  */

import org.apache.spark.sql.{SparkSession}
import ch.epfl.lts2.Utils._
import ch.epfl.lts2.Globals._

object WikiPageCountsParser extends App {

  suppressLogs(List("org", "akka"))

  val spark = SparkSession.builder
    .master("local[*]")
    .appName("Wiki PageCounts Parser")
    .config("spark.driver.maxResultSize", "20g") // change this if needed
    .config("spark.executor.memory", "50g") // change this if needed
    .getOrCreate()

  import spark.implicits._

  val SQLContext = spark.sqlContext

  val YEAR = "2018-"
  val MONTH = "01-"
  val DAY = "03"
  val PROJECT = "en.z" // Wikipedia

  val sc = spark.sparkContext

  case class Record(project: String, page: String, dailyTotal: Int, hourlyCounts: String)

    var t = sc.textFile(PATH_RESOURCES + "pagecounts-" + YEAR + MONTH + "*" + ".bz2")
      .filter(line => !line.contains("#"))
      .map(_.split(" "))
      .map {
        case Array(project, page, dailyTotal, hourlyCounts) => Record(project, page, dailyTotal.toInt, hourlyCounts)
      }
      .toDF()
//      .filter($"dailyTotal" > 100)
      .filter($"project" === PROJECT)

  println(t.count())

}
