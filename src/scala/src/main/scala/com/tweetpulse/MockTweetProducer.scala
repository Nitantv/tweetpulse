package com.tweetpulse

import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.StringSerializer
import java.util.Properties
import scala.util.Random

object MockTweetProducer {

  def buildConfig(): Properties = {
    val props = new Properties()
    props.put("bootstrap.servers", sys.env.getOrElse("KAFKA_BOOTSTRAP", ""))
    props.put("security.protocol", "SASL_SSL")
    props.put("sasl.mechanism", "PLAIN")
    props.put("sasl.jaas.config",
      s"""org.apache.kafka.common.security.plain.PlainLoginModule required username="${sys.env.getOrElse("KAFKA_KEY", "")}" password="${sys.env.getOrElse("KAFKA_SECRET", "")}";""")
    props.put("key.serializer", classOf[StringSerializer].getName)
    props.put("value.serializer", classOf[StringSerializer].getName)
    props
  }

  def generateTweet(id: Long): Tweet = {
    val random = new Random()
    val topics = List(
      ("Spark is amazing for big data! #databricks #spark", List("databricks", "spark")),
      ("PySpark makes data engineering easier #python #bigdata", List("python", "bigdata")),
      ("Delta Lake is the future #deltalake #databricks", List("deltalake", "databricks")),
      ("Kafka + Spark Streaming = real-time magic #kafka #streaming", List("kafka", "streaming")),
      ("MLflow makes model tracking effortless #mlflow #ml", List("mlflow", "ml")),
      ("Unity Catalog is a game changer #databricks", List("databricks")),
      ("Scala is elegant for Spark development #scala #spark", List("scala", "spark")),
      ("Just built my first streaming pipeline! #spark", List("spark")),
      ("Bronze Silver Gold medallion architecture #lakehouse", List("lakehouse")),
      ("Real-time sentiment analysis with Spark ML #ml", List("ml", "streaming"))
    )
    val langs = List("en", "en", "en", "fr", "de", "es", "ja")
    val (text, hashtags) = topics(random.nextInt(topics.length))
    val lang = langs(random.nextInt(langs.length))
    Tweet(
      id             = id.toString,
      text           = text,
      authorId       = s"user_${random.nextInt(1000)}",
      createdAt      = java.time.Instant.now().toString,
      lang           = Some(lang),
      metrics        = TweetMetrics(
        random.nextInt(2000), random.nextInt(500),
        random.nextInt(100), random.nextInt(100000).toLong
      ),
      hashtags       = hashtags,
      mentionedUsers = List.empty
    )
  }

  def toJson(tweet: Tweet): String =
    s"""{"id":"${tweet.id}","text":"${tweet.text.replace("\"","'")}","author_id":"${tweet.authorId}","created_at":"${tweet.createdAt}","lang":"${tweet.lang.getOrElse("unknown")}","like_count":${tweet.metrics.likeCount},"retweet_count":${tweet.metrics.retweetCount},"reply_count":${tweet.metrics.replyCount},"impression_count":${tweet.metrics.impressionCount},"hashtags":[${tweet.hashtags.map(h => s""""$h"""").mkString(",")}]}"""

  def produce(topic: String, count: Int, delayMs: Int): Unit = {
    val props    = buildConfig()
    val producer = new KafkaProducer[String, String](props)
    println(s"Connecting to: ${props.get("bootstrap.servers")}")
    println(s"Sending $count tweets to: $topic")
    println("=" * 60)
    var sent = 0
    var failed = 0
    (1 to count).foreach { i =>
      val tweet  = generateTweet(i)
      val record = new ProducerRecord[String, String](topic, tweet.id, toJson(tweet))
      producer.send(record, (meta: RecordMetadata, ex: Exception) => {
        if (ex == null) {
          sent += 1
          println(s"[$i/$count] Sent: ${tweet.id} → partition=${meta.partition()} offset=${meta.offset()}")
        } else {
          failed += 1
          println(s"[$i/$count] FAILED: ${ex.getMessage}")
        }
      })
      if (delayMs > 0) Thread.sleep(delayMs)
    }
    producer.flush()
    producer.close()
    println("=" * 60)
    println(s"Done! Sent: $sent  Failed: $failed")
  }

  def main(args: Array[String]): Unit = {
    val topic   = "tweetpulse.raw.tweets"
    val count   = if (args.length > 0) args(0).toInt else 20
    val delayMs = if (args.length > 1) args(1).toInt else 500
    produce(topic, count, delayMs)
  }
}
