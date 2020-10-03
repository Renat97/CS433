
import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.matching.Regex

object AssignmentTwo {
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
    .setAppName("AssignmentTwo")

  val sc = new SparkContext(conf)

  val tweets = sc.textFile(tweetsFilePath)

  val cachedRetweets = tweets
    .flatMap(line => line.split("\t"))
    .filter(_.startsWith("RT"))
    .cache()

  val regexRemovePunctuation = "[,.!?:;)(-]"

    cachedRetweets
      .map(_.replaceAll(regexRemovePunctuation, "")
        .trim
        .toLowerCase)
      .flatMap(mention => findRetweetedHandle(mention))
      .map((_, 1))
      .reduceByKey(_ + _)
      .sortBy(-_._2)
      .take(10)
      .foreach(println)
  }

  def findRetweetedHandle (tweetLine: String): Option[String] = {

    val mentionPattern: Regex = "\\s([@][\\w_-]+)".r
     mentionPattern findFirstIn tweetLine
  }

}

