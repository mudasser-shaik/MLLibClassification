package Streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by mshaik on 9/6/16.
  */
object TwitterSparkStreaming {

  def main(args: Array[String]) {

    //access TwitterKeys
    val config = new java.util.Properties()
    config.load(this.getClass().getClassLoader().getResourceAsStream("config.properties"))
    System.setProperty("twitter4j.oauth.consumerKey", config.getProperty("twitter_consumerKey").toString)
    System.setProperty("twitter4j.oauth.consumerSecret", config.getProperty("twitter_consumerSecret").toString)
    System.setProperty("twitter4j.oauth.accessToken", config.getProperty("twitter_accessToken").toString)
    System.setProperty("twitter4j.oauth.accessTokenSecret", config.getProperty("twitter_accessTokenSecret").toString)

    // Twitter Tags
    val filterTags = Array("@realDonaldTrump","@ESPN")

    //Setup SparkStreaming context with all CPU cores and 2 sec batches of Data
    val sparkConf = new SparkConf().setAppName("TwitterPopularTags").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    //Create DStream from Twitter using StreamingContext and filterTags
    val tweetStreams = TwitterUtils.createStream(ssc, None, filterTags)

    // All the Data from TwitterData.
    tweetStreams.print()

    // Get Tweet Text and Split each word by Space and Filter the HashTags in the Tweet
    val hashTags = tweetStreams.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))
    hashTags.print()

    // Read in the word-sentiment from AFINN-111 list and create a static RDD from it
    val wordSentiments = ssc.sparkContext.textFile("AFINN-111.txt").map { line =>
      val Array(word, happinessValue) = line.split("\t")
      (word, happinessValue.toInt)
    }.cache()


    /* Determine the hash tags with the highest sentiment values
       1. Joining the streaming RDD of twitterHashTags with the static RDD inside the transform() method and
       2. multiplying the frequency of the hash tag by its sentiment value
    */
    val Positive60 =  hashTags.map(hashTag => (hashTag.tail, 1))
                              .reduceByKeyAndWindow(_ + _, Seconds(60))
                              .transform{topicCount => wordSentiments.join(topicCount)}
                              .map{case (topic, tuple) => (topic, tuple._1 * tuple._2)}
                              .map{case (topic, happinessValue) => (happinessValue, topic)}
                              .transform(_.sortByKey(false))


    // Print hash tags with the most positive sentiment values
    Positive60.foreachRDD(rdd => {
      val topList = rdd.take(10)
      println("\nHappiest topics in last 60 seconds (%s total):".format(rdd.count()))
      topList.foreach{case (happiness, tag) => println("%s (%s happiness)".format(tag, happiness))}
    })



    /*
        Filter Out Removing StopWords from Dstream
        Join the tweetDstream with StaticRDD (wordSentiments)
        Aggregating the Text Sentiments aganist the HashTag/userID
     */


    ssc.start()
    ssc.awaitTermination()
  }
}
