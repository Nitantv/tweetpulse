package com.tweetpulse

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class MockTweetProducerSpec extends AnyFlatSpec with Matchers {

  "generateTweet" should "produce tweet with valid id" in {
    val tweet = MockTweetProducer.generateTweet(42)
    tweet.id shouldEqual "42"
  }

  it should "produce tweet with non-empty text" in {
    val tweet = MockTweetProducer.generateTweet(1)
    tweet.text should not be empty
  }

  it should "produce tweet with valid lang Option" in {
    val tweet = MockTweetProducer.generateTweet(1)
    tweet.lang shouldBe defined
    List("en","fr","de","es","ja") should contain(tweet.lang.get)
  }

  it should "produce tweet with non-empty hashtags" in {
    val tweet = MockTweetProducer.generateTweet(1)
    tweet.hashtags should not be empty
  }

  it should "produce tweet with positive metrics" in {
    val tweet = MockTweetProducer.generateTweet(1)
    tweet.metrics.likeCount      should be >= 0
    tweet.metrics.retweetCount   should be >= 0
    tweet.metrics.replyCount     should be >= 0
    tweet.metrics.impressionCount should be >= 0L
  }

  it should "produce unique tweets for different ids" in {
    val t1 = MockTweetProducer.generateTweet(1)
    val t2 = MockTweetProducer.generateTweet(2)
    t1.id should not equal t2.id
  }

  "toJson" should "produce valid JSON string" in {
    val tweet = MockTweetProducer.generateTweet(1)
    val json  = MockTweetProducer.toJson(tweet)
    json should include("\"id\"")
    json should include("\"text\"")
    json should include("\"author_id\"")
    json should include("\"like_count\"")
    json should include("\"hashtags\"")
  }

  it should "include correct tweet id in JSON" in {
    val tweet = MockTweetProducer.generateTweet(99)
    val json  = MockTweetProducer.toJson(tweet)
    json should include("\"99\"")
  }

  it should "produce JSON with hashtag array" in {
    val tweet = MockTweetProducer.generateTweet(1)
    val json  = MockTweetProducer.toJson(tweet)
    json should include("[")
    json should include("]")
  }

  it should "escape quotes in tweet text" in {
    val tweet = MockTweetProducer.generateTweet(1)
    val json  = MockTweetProducer.toJson(tweet)
    json should not include "\\\""
  }

  "buildConfig" should "set bootstrap servers from env" in {
    val props = MockTweetProducer.buildConfig()
    props.getProperty("bootstrap.servers") should not be empty
  }

  it should "set SASL_SSL security protocol" in {
    val props = MockTweetProducer.buildConfig()
    props.getProperty("security.protocol") shouldEqual "SASL_SSL"
  }

  it should "set PLAIN sasl mechanism" in {
    val props = MockTweetProducer.buildConfig()
    props.getProperty("sasl.mechanism") shouldEqual "PLAIN"
  }

  it should "set string serializers" in {
    val props = MockTweetProducer.buildConfig()
    props.getProperty("key.serializer") should include("StringSerializer")
    props.getProperty("value.serializer") should include("StringSerializer")
  }
}
