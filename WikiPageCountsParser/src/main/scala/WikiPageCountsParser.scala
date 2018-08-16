/**
  * Created by volodymyrmiz on 16/08/18.
  */

import org.apache.spark.sql.SparkSession
import ch.epfl.lts2.Utils._
import ch.epfl.lts2.Globals._

object WikiPageCountsParser extends App{
  val spark = SparkSession.builder
    .master("local[*]")
    .appName("Wiki PageCounts Parser")
    .getOrCreate()

  suppressLogs(List("org", "akka"))
}
