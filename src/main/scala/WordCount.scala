import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by mudasser on 01/06/16.
  */
object WordCount {

  def main(args: Array[String]) {

    val conf = new SparkConf()
                .setAppName("WordCount")
                  .setMaster("local[*]")
               .set("spark.executor.memory","2g")

    val sc = new SparkContext(conf)
    val lines = sc.textFile("Readme.txt")
    val count = lines.map(word => (word,1)).reduceByKey(_+_)
    count.foreach(println)
  }

}
