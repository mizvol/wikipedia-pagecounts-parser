/**
  * Created by volodymyrmiz on 16/08/18.
  */

import java.util.Calendar

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{collect_list, typedLit}
import ch.epfl.lts2.Utils._
import ch.epfl.lts2.Globals._
import org.slf4j.{Logger, LoggerFactory}


object WikiPageCountsParser extends App {

  suppressLogs(List("org", "akka"))

  val log: Logger = LoggerFactory.getLogger(this.getClass)

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
  val DAYS = 31
  val PROJECT = "en.z" // Wikipedia

  val sc = spark.sparkContext

  log.info("Start time: " + Calendar.getInstance().getTime())

  // Initial record format
  case class Record(project: String, page: String, dailyTotal: Int, hourlyCounts: String)

  // Resulting record format. Added a column for the dates
  case class RecordDF(project: String, page: String, dailyTotal: Int, hourlyCounts: String, day: String)

  var df = spark.emptyDataset[RecordDF].toDF()

  // Read files in the resources folder one by one. Specify YEAR, MONTH, and DAYS (number of days in the month)
  // according to the file names.
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
      .filter($"dailyTotal" > 100)
      .filter($"project" === PROJECT)
    t = t.withColumn("day", typedLit[String](YEAR + MONTH + day))
    df = df.union(t)
  }

  df = df.drop($"project")

  val df1 = df.groupBy($"page").agg(collect_list($"dailyTotal"), collect_list($"day"))

  df1.show()
  println(df1.count())

  log.info("End time: " + Calendar.getInstance().getTime())
}
