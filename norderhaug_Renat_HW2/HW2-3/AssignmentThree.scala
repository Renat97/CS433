import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{col, to_date}
import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, unix_timestamp}

object AssignmentThree {
  def main(args: Array[String]) {

  val log = Logger.getLogger(this.getClass)

  if (args.length < 2) {
    println("\nArguments: <TWEETS_FILE_PATH> <USERS_FILE_PATH> " +
      "\nFound: " + args.mkString("\n   "))
  }

  var Array(tweetsFilePath, usersFilePath) = args

  log.info(
    s"""| Using:
        | tweetsFilePath:     $tweetsFilePath
        | usersFilePath:      $usersFilePath
     """.stripMargin
  )

  println("Arguments: " +
    "tweetsFilePath: " + tweetsFilePath +
    "usersFilePath: "  + usersFilePath )

  val sparkSession =
    SparkSession.builder()
      .appName("AssignmentThree")
      .getOrCreate()


  val tweetsDf = sparkSession.read
    .option("header", "false")
    .option("delimiter", "\t")
    .csv(tweetsFilePath)
    .toDF("userIdString","tweetIdString","tweetText","tweetTimestampString")

  val usersDf =  sparkSession.read
    .option("header", "false")
    .option("delimiter", "\t")
    .csv(usersFilePath)
    .toDF("userIdString","userLocation")

  val joinedDf =  tweetsDf.join(usersDf, Seq("userIdString", "userIdString"), "inner")

    joinedDf
      .filter(to_date(joinedDf("tweetTimestampString")) >= "2009-09-16")
      .filter(to_date(joinedDf("tweetTimestampString")) <= "2009-09-20")
      .filter(col("userLocation") contains "Los Angeles")
      .select(col("userIdString"))
      .groupBy("userIdString").count()
      .orderBy(col("count").desc)
      .limit(10)
      .show()

  }

}

