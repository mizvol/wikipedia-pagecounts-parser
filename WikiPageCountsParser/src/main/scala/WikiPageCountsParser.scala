/**
  * Created by volodymyrmiz on 16/08/18.
  */

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.typedLit
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
  val DAYS = 5
  val PROJECT = "en.z" // Wikipedia

  val sc = spark.sparkContext

  // Initial record format
  case class Record(project: String, page: String, dailyTotal: Int, hourlyCounts: String)

  // Resulting record format with dates as a column
  case class RecordDF(project: String, page: String, dailyTotal: Int, hourlyCounts: String, day: String)

  var df = spark.emptyDataset[RecordDF].toDF()

  for (i <- 1 to DAYS) {
    var day = i.toString
    if (i <= 9) day = "0" + i.toString

    var t = sc.textFile(PATH_RESOURCES + "pagecounts-" + YEAR + MONTH + day + ".bz2")
      .filter(line => !line.contains("#"))
      .map(_.split(" "))
      .map {
        case Array(project, page, dailyTotal, hourlyCounts) => Record(project, page, dailyTotal.toInt, hourlyCounts)
      }
      .toDF()
//      .filter($"dailyTotal" > 100)
      .filter($"project" === PROJECT)
    t = t.withColumn("day", typedLit[String](YEAR + MONTH + day))
    df = df.union(t)
  }

  df = df.drop($"project")

  df.show()
}
