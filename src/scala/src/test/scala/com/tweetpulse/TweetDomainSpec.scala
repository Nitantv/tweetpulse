package com.tweetpulse

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import scala.util.{Success, Failure}

class TweetDomainSpec extends AnyFlatSpec with Matchers {

  // ── Fixtures ──────────────────────────────────────────────
  val metrics = TweetMetrics(500, 100, 20, 10000L)
  val tweet1  = Tweet("1", "Spark is amazing! #databricks", "u1",
    "2026-04-29", Some("en"), metrics, List("databricks"), List("u2"))
  val tweet2  = Tweet("2", "PySpark rocks", "u2",
    "2026-04-29", Some("en"), TweetMetrics(1500,300,50,50000L), List(), List())
  val tweets  = List(tweet1, tweet2)

  // ── Case Class Tests ──────────────────────────────────────
  "A Tweet case class" should "support structural equality" in {
    tweet1 shouldEqual tweet1.copy()
  }

  it should "support copy with modification" in {
    val modified = tweet1.copy(text = "Updated text")
    modified.text shouldEqual "Updated text"
    modified.id   shouldEqual tweet1.id
  }

  it should "handle Option fields correctly" in {
    tweet1.lang shouldEqual Some("en")
    Tweet.empty.lang shouldEqual None
  }

  // ── Companion Object Tests ────────────────────────────────
  "Tweet.fromMap" should "parse valid map successfully" in {
    val result = Tweet.fromMap(Map(
      "id" -> "10", "text" -> "Hello", 
      "author_id" -> "u1", "created_at" -> "2026-04-29"
    ))
    result shouldBe a[Success[_]]
    result.get.id shouldEqual "10"
  }

  it should "return Failure when required field is missing" in {
    val result = Tweet.fromMap(Map("id" -> "1"))
    result shouldBe a[Failure[_]]
  }

  it should "parse hashtags correctly" in {
    val result = Tweet.fromMap(Map(
      "id" -> "1", "text" -> "Hello",
      "author_id" -> "u1", "created_at" -> "2026-04-29",
      "hashtags" -> "spark,scala,databricks"
    ))
    result.get.hashtags shouldEqual List("spark", "scala", "databricks")
  }

  // ── Sealed Trait + Pattern Matching Tests ─────────────────
  "EventProcessor" should "handle all PipelineEvent variants" in {
    EventProcessor.process(TweetReceived(tweet1)) should include("Processing tweet 1")
    EventProcessor.process(TweetFiltered("dup"))  should include("filtered")
    EventProcessor.process(TweetFailed("err"))    should include("failed")
    EventProcessor.process(BatchCompleted(5))     should include("5 tweets")
  }

  it should "extract language from Option correctly" in {
    EventProcessor.extractLang(tweet1)                    shouldEqual "Language: en"
    EventProcessor.extractLang(tweet1.copy(lang = None))  shouldEqual "Language: unknown"
  }

  // ── Validator + Either Tests ──────────────────────────────
  "TweetValidator" should "return Right for valid tweet" in {
    val result = new TweetValidator().validate(tweet1)
    result.isRight shouldBe true
  }

  it should "return Left when ID is empty" in {
    val result = new TweetValidator().validate(Tweet.empty)
    result.isLeft shouldBe true
    result.left.get should include("ID")
  }

  it should "return Left when text is too long" in {
    val longTweet = tweet1.copy(text = "a" * 281)
    val result    = new TweetValidator().validate(longTweet)
    result.isLeft shouldBe true
    result.left.get should include("too long")
  }

  // ── Higher Order Function Tests ───────────────────────────
  "TweetAnalytics" should "extract texts correctly" in {
    TweetAnalytics.extractTexts(tweets) shouldEqual 
      List("Spark is amazing! #databricks", "PySpark rocks")
  }

  it should "filter by language" in {
    val frTweet = tweet1.copy(id = "3", lang = Some("fr"))
    TweetAnalytics.filterByLang(tweets :+ frTweet, "en").map(_.id) shouldEqual List("1", "2")
  }

  it should "collect all hashtags via flatMap" in {
    TweetAnalytics.allHashtags(tweets) shouldEqual List("databricks")
  }

  it should "sum total likes via foldLeft" in {
    TweetAnalytics.totalLikes(tweets) shouldEqual 2000
  }

  it should "find max likes via reduce" in {
    TweetAnalytics.maxLikes(tweets) shouldEqual 1500
  }

  it should "return top N tweets by likes" in {
    TweetAnalytics.topTweets(tweets, 1).head.id shouldEqual "2"
  }

  it should "collect only TweetReceived events" in {
    val events = List(TweetReceived(tweet1), TweetFiltered("dup"), TweetReceived(tweet2))
    TweetAnalytics.receivedOnly(events).map(_.id) shouldEqual List("1", "2")
  }

  it should "zip tweets with index starting at 1" in {
    val result = TweetAnalytics.withIndex(tweets)
    result.head._1 shouldEqual 1
    result.last._1 shouldEqual 2
  }

  it should "filter engaged english tweets via for comprehension" in {
    val result = TweetAnalytics.engagedEnglishTweets(tweets)
    result.map(_.id) shouldEqual List("1", "2")
  }

  // ── For Comprehension Tests ───────────────────────────────
  "ForComprehensions" should "return Some when both lang and hashtag exist" in {
    val result = ForComprehensions.getEngagement(tweet1)
    result shouldEqual Some("en tweet with hashtag #databricks")
  }

  it should "return None when hashtags list is empty" in {
    val result = ForComprehensions.getEngagement(tweet1.copy(hashtags = List()))
    result shouldEqual None
  }

  it should "return None when lang is None" in {
    val result = ForComprehensions.getEngagement(tweet1.copy(lang = None))
    result shouldEqual None
  }

  // ── Implicit Tests ────────────────────────────────────────
  "TweetOps implicits" should "correctly identify viral tweets" in {
    import Implicits._
    tweet1.isViral shouldBe false  // 500 likes
    tweet2.isViral shouldBe true   // 1500 likes
  }

  it should "generate correct summary" in {
    import Implicits._
    tweet1.summary should include("1")
    tweet1.summary should include("500 likes")
  }

  it should "convert tweet to map with correct keys" in {
    import Implicits._
    val m = tweet1.toMap
    m("id")        shouldEqual "1"
    m("like_count") shouldEqual "500"
  }

  "RichList implicits" should "return Some for non-empty list safeHead" in {
    import Implicits._
    List("a", "b").safeHead shouldEqual Some("a")
  }

  it should "return None for empty list safeHead" in {
    import Implicits._
    List[String]().safeHead shouldEqual None
  }
}
