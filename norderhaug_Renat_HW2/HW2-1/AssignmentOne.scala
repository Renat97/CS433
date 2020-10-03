
import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.Logger

import scala.util.matching.Regex

object AssignmentOne {
  def main(args: Array[String]) {
val log = Logger.getLogger(this.getClass)

  if (args.length < 1) {
    println("\nArguments: <TWEETS_FILE_PATH> " +
      "\nFound: " + args.mkString("\n   "))
  }

  var Array(tweetsFilePath) = args

  log.info(
    s"""| Using:
        | tweetsFilePath:     $tweetsFilePath
     """.stripMargin
  )

  println("Arguments: " +
    "tweetsFilePath: " + tweetsFilePath)

  val conf: SparkConf = new SparkConf()
    .setAppName("AssignmentOne")

  val sc = new SparkContext(conf)

  val tweets = sc.textFile(tweetsFilePath)

  val cachedTweetsWithMentions = tweets
    .flatMap(line => line.split("\t"))
    .filter(_.contains("@"))
    .cache()

  val regexRemovePunctuation = "[,.!?:;)(-]"

  def parseLine(tweetLine: String): Set[String] = {

    val mentionPattern: Regex = "\\s([@][\\w_-]+)".r

    tweetLine
      .flatMap(line => mentionPattern findAllIn tweetLine)
      .filter(_.length > 1)
      .toSet[String]
  }

      cachedTweetsWithMentions
      .flatMap(mention => parseLine(mention))
      .map(_.replaceAll(regexRemovePunctuation, "")
        .trim
        .toLowerCase)
      .map((_, 1))
      .reduceByKey(_ + _)
      .sortBy(-_._2)
      .take(20)
      .foreach(println)
  }
}
