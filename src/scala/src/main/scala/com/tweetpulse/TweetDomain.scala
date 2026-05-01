package com.tweetpulse

import scala.util.{Try, Success, Failure}

// ─────────────────────────────────────────────────────────────
// SECTION 1: CASE CLASSES
// Immutable data models — backbone of the entire pipeline
// ─────────────────────────────────────────────────────────────

case class TweetMetrics(
  likeCount: Int,
  retweetCount: Int,
  replyCount: Int,
  impressionCount: Long
)

case class User(
  id: String,
  username: String,
  followersCount: Int,
  verified: Boolean
)

case class Tweet(
  id: String,
  text: String,
  authorId: String,
  createdAt: String,
  lang: Option[String],         // Option = nullable field
  metrics: TweetMetrics,
  hashtags: List[String],
  mentionedUsers: List[String]
)

// ─────────────────────────────────────────────────────────────
// SECTION 2: COMPANION OBJECT
// Factory methods, constants, apply() overloads
// ─────────────────────────────────────────────────────────────

object Tweet {

  // Factory method — create from raw map (simulates JSON parsing)
  def fromMap(m: Map[String, String]): Try[Tweet] = Try {
    Tweet(
      id        = m("id"),
      text      = m("text"),
      authorId  = m("author_id"),
      createdAt = m("created_at"),
      lang      = m.get("lang"),           // returns Option[String]
      metrics   = TweetMetrics(
        likeCount       = m.getOrElse("like_count", "0").toInt,
        retweetCount    = m.getOrElse("retweet_count", "0").toInt,
        replyCount      = m.getOrElse("reply_count", "0").toInt,
        impressionCount = m.getOrElse("impression_count", "0").toLong
      ),
      hashtags       = m.getOrElse("hashtags", "").split(",").filter(_.nonEmpty).toList,
      mentionedUsers = m.getOrElse("mentions", "").split(",").filter(_.nonEmpty).toList
    )
  }

  // Empty tweet for testing
  def empty: Tweet = Tweet(
    id = "", text = "", authorId = "", createdAt = "",
    lang = None,
    metrics = TweetMetrics(0, 0, 0, 0L),
    hashtags = List.empty,
    mentionedUsers = List.empty
  )
}

// ─────────────────────────────────────────────────────────────
// SECTION 3: SEALED TRAIT + ADT
// Represents all possible pipeline events — exhaustive matching
// ─────────────────────────────────────────────────────────────

sealed trait PipelineEvent
case class TweetReceived(tweet: Tweet)       extends PipelineEvent
case class TweetFiltered(reason: String)     extends PipelineEvent
case class TweetFailed(error: String)        extends PipelineEvent
case class BatchCompleted(count: Int)        extends PipelineEvent

// ─────────────────────────────────────────────────────────────
// SECTION 4: TRAITS + MIXINS
// ─────────────────────────────────────────────────────────────

trait Validator[A] {
  def validate(value: A): Either[String, A]
}

trait Logger {
  def log(msg: String): Unit = println(s"[LOG] $msg")
}

// Mixin: TweetValidator uses both Validator trait and Logger trait
class TweetValidator extends Validator[Tweet] with Logger {
  def validate(tweet: Tweet): Either[String, Tweet] = {
    if (tweet.id.isEmpty)
      Left("Tweet ID cannot be empty")
    else if (tweet.text.isEmpty)
      Left("Tweet text cannot be empty")
    else if (tweet.text.length > 280)
      Left(s"Tweet too long: ${tweet.text.length} chars")
    else {
      log(s"Tweet ${tweet.id} is valid")
      Right(tweet)
    }
  }
}

// ─────────────────────────────────────────────────────────────
// SECTION 5: PATTERN MATCHING
// ─────────────────────────────────────────────────────────────

object EventProcessor {

  def process(event: PipelineEvent): String = event match {
    case TweetReceived(tweet)    => s"Processing tweet ${tweet.id}: ${tweet.text.take(50)}"
    case TweetFiltered(reason)   => s"Tweet filtered: $reason"
    case TweetFailed(error)      => s"Tweet failed: $error"
    case BatchCompleted(count)   => s"Batch done: $count tweets processed"
  }

  // Pattern matching on Option
  def extractLang(tweet: Tweet): String = tweet.lang match {
    case Some(l) => s"Language: $l"
    case None    => "Language: unknown"
  }

  // Pattern matching on Try
  def safeParse(data: Map[String, String]): String =
    Tweet.fromMap(data) match {
      case Success(tweet)  => s"Parsed tweet: ${tweet.id}"
      case Failure(error)  => s"Parse failed: ${error.getMessage}"
    }

  // Pattern matching on Either
  def safeValidate(tweet: Tweet): String = {
    val validator = new TweetValidator
    validator.validate(tweet) match {
      case Right(valid) => s"Valid tweet: ${valid.id}"
      case Left(error)  => s"Invalid tweet: $error"
    }
  }
}

// ─────────────────────────────────────────────────────────────
// SECTION 6: HIGHER ORDER FUNCTIONS + COLLECTIONS
// ─────────────────────────────────────────────────────────────

object TweetAnalytics {

  // map — transform each tweet
  def extractTexts(tweets: List[Tweet]): List[String] =
    tweets.map(_.text)

  // filter — keep only tweets matching condition
  def filterByLang(tweets: List[Tweet], lang: String): List[Tweet] =
    tweets.filter(_.lang.contains(lang))

  // flatMap — flatten nested lists
  def allHashtags(tweets: List[Tweet]): List[String] =
    tweets.flatMap(_.hashtags)

  // fold — aggregate to single value
  def totalLikes(tweets: List[Tweet]): Int =
    tweets.foldLeft(0)((acc, t) => acc + t.metrics.likeCount)

  // reduce — combine without initial value
  def maxLikes(tweets: List[Tweet]): Int =
    tweets.map(_.metrics.likeCount).reduce(_ max _)

  // groupBy — group tweets by language
  def groupByLang(tweets: List[Tweet]): Map[Option[String], List[Tweet]] =
    tweets.groupBy(_.lang)

  // sortBy — order tweets by like count
  def topTweets(tweets: List[Tweet], n: Int): List[Tweet] =
    tweets.sortBy(_.metrics.likeCount)(Ordering[Int].reverse).take(n)

  // collect — pattern match + filter in one step
  def receivedOnly(events: List[PipelineEvent]): List[Tweet] =
    events.collect { case TweetReceived(t) => t }

  // zip + zipWithIndex
  def withIndex(tweets: List[Tweet]): List[(Int, Tweet)] =
    tweets.zipWithIndex.map { case (t, i) => (i + 1, t) }

  // for comprehension — like flatMap + filter combined
  def engagedEnglishTweets(tweets: List[Tweet]): List[Tweet] =
    for {
      tweet <- tweets
      if tweet.lang.contains("en")
      if tweet.metrics.likeCount > 10
    } yield tweet
}

// ─────────────────────────────────────────────────────────────
// SECTION 7: FOR COMPREHENSIONS
// ─────────────────────────────────────────────────────────────

object ForComprehensions {

  // For comprehension over Option — safe chaining
  def getEngagement(tweet: Tweet): Option[String] =
    for {
      lang    <- tweet.lang
      hashtag <- tweet.hashtags.headOption
    } yield s"$lang tweet with hashtag #$hashtag"

  // For comprehension over Either — error propagation
  def processAndValidate(
    data: Map[String, String]
  ): Either[String, String] =
    for {
      tweet   <- Tweet.fromMap(data).toEither.left.map(_.getMessage)
      valid   <- new TweetValidator().validate(tweet)
    } yield s"Processed: ${valid.id}"
}

// ─────────────────────────────────────────────────────────────
// SECTION 8: IMPLICIT CONVERSIONS
// ─────────────────────────────────────────────────────────────

object Implicits {
  // Implicit class adds methods to existing types
  implicit class TweetOps(tweet: Tweet) {
    def isViral: Boolean = tweet.metrics.likeCount > 1000
    def summary: String  = s"[${tweet.id}] ${tweet.text.take(30)}... (${tweet.metrics.likeCount} likes)"
    def toMap: Map[String, String] = Map(
      "id"           -> tweet.id,
      "text"         -> tweet.text,
      "author_id"    -> tweet.authorId,
      "created_at"   -> tweet.createdAt,
      "like_count"   -> tweet.metrics.likeCount.toString
    )
  }

  implicit class RichList[A](list: List[A]) {
    def safeHead: Option[A] = list.headOption
    def safeLast: Option[A] = list.lastOption
  }
}

// ─────────────────────────────────────────────────────────────
// MAIN — run all sections
// ─────────────────────────────────────────────────────────────

object Main extends App {
  import Implicits._

  println("=" * 60)
  println("TWEETPULSE — Scala Fundamentals")
  println("=" * 60)

  // Section 1 & 2: Case classes + companion object
  println("\n--- Case Classes ---")
  val t1 = Tweet("1", "Spark is amazing! #databricks", "u1", "2026-04-29",
    Some("en"), TweetMetrics(500, 100, 20, 10000L), List("databricks"), List("u2"))
  val t2 = Tweet("2", "PySpark rocks #python #bigdata", "u2", "2026-04-29",
    Some("en"), TweetMetrics(1500, 300, 50, 50000L), List("python", "bigdata"), List())
  val t3 = Tweet("3", "Bonjour le monde", "u3", "2026-04-29",
    Some("fr"), TweetMetrics(10, 2, 1, 500L), List(), List())
  val tweets = List(t1, t2, t3)
  println(t1)
  println(s"t1 == t1.copy: ${t1 == t1.copy()}")

  // fromMap + Try
  println("\n--- Try + fromMap ---")
  val good = Tweet.fromMap(Map("id"->"4","text"->"Hello","author_id"->"u4","created_at"->"2026-04-29"))
  val bad  = Tweet.fromMap(Map("id"->"5"))  // missing required fields → Failure
  println(s"Good parse: $good")
  println(s"Bad parse: $bad")

  // Section 3: Sealed traits + pattern matching
  println("\n--- Pattern Matching ---")
  val events: List[PipelineEvent] = List(
    TweetReceived(t1),
    TweetFiltered("duplicate"),
    TweetFailed("network timeout"),
    BatchCompleted(3)
  )
  events.foreach(e => println(EventProcessor.process(e)))
  println(EventProcessor.extractLang(t1))
  println(EventProcessor.extractLang(t3.copy(lang = None)))

  // Section 4: Traits + validation
  println("\n--- Validation (Either) ---")
  println(EventProcessor.safeValidate(t1))
  println(EventProcessor.safeValidate(Tweet.empty))

  // Section 6: Higher order functions
  println("\n--- Higher Order Functions ---")
  println(s"Texts: ${TweetAnalytics.extractTexts(tweets)}")
  println(s"English: ${TweetAnalytics.filterByLang(tweets, "en").map(_.id)}")
  println(s"All hashtags: ${TweetAnalytics.allHashtags(tweets)}")
  println(s"Total likes: ${TweetAnalytics.totalLikes(tweets)}")
  println(s"Max likes: ${TweetAnalytics.maxLikes(tweets)}")
  println(s"Top 2: ${TweetAnalytics.topTweets(tweets, 2).map(_.id)}")
  println(s"Received only: ${TweetAnalytics.receivedOnly(events).map(_.id)}")
  println(s"With index: ${TweetAnalytics.withIndex(tweets).map { case (i,t) => s"$i:${t.id}"}}")
  println(s"Engaged EN: ${TweetAnalytics.engagedEnglishTweets(tweets).map(_.id)}")

  // Section 7: For comprehensions
  println("\n--- For Comprehensions ---")
  println(ForComprehensions.getEngagement(t1))
  println(ForComprehensions.getEngagement(t3.copy(hashtags = List())))

  // Section 8: Implicits
  println("\n--- Implicits ---")
  println(s"t1 viral: ${t1.isViral}")
  println(s"t2 viral: ${t2.isViral}")
  println(s"t1 summary: ${t1.summary}")
  println(s"t1 toMap: ${t1.toMap}")
  println(s"hashtags safeHead: ${t1.hashtags.safeHead}")
  println(s"empty safeHead: ${List[String]().safeHead}")

  println("\n" + "=" * 60)
  println("ALL SECTIONS COMPLETE")
  println("=" * 60)
}
