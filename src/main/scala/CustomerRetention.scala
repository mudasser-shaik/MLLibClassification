import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by mudasser on 12/06/16.
  *
  * Data Challenge to offer new Schemes to the Customers who are 100kms away from the Dublin Office.
  * Spark Job to run on the Customer.txt with Geo Spactial Data.
  * Calculate the distance using latitudes and Longitutes
  *
  */
object CustomerRetention {

  case class Customer(latitude: Double, longitude: Double, name : String , user_id: Long, distance: Double)

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("IntercomCutomerRetention")
      .setMaster("local[*]")
      .set("spark.executor.memory","4g")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // load text file into DataFrames
    val dfCustomer = sqlContext.read.json("customer.txt")


    import scala.math._

    /* Given Office Dublin( 53.3381985, -6.2592576)
       Formula : Law of Cosines where R = 6371.0 kms
       d = acos( sin φ1 ⋅ sin φ2 + cos φ1 ⋅ cos φ2 ⋅ cos Δλ ) ⋅ R
    */
    def distanceGeoFrom(lat : Double, lon: Double ): Double = {

      val latOffice = 53.3381985
      val lonOffice = -6.2592576
      val difflon = (lonOffice - lon).toRadians
      val centralAngle = acos(sin(latOffice.toRadians)* sin(lat.toRadians) + cos(latOffice.toRadians) * cos(lat.toRadians) * cos(difflon))

      centralAngle * 6371.0
    }

    val dfFeatures = dfCustomer.map {
      line =>

      val lat = line.getAs[String]("latitude").toDouble
      val lon = line.getAs[String]("longitude").toDouble

        //adding the distance column.
      Customer(lat,lon,line.getAs[String]("name"),line.getAs[Long]("user_id"),distanceGeoFrom(lat,lon))
    }

    val df = sqlContext.createDataFrame(dfFeatures).cache()

    df.registerTempTable("customerTable")

    // returns Customers who are with in 100kms
    df.sqlContext.sql("SELECT user_id, name FROM customerTable WHERE distance <=100 SORT BY user_id").show()

/*    import org.apache.spark.sql.functions.udf
    val toDouble = udf[Double, String]( _.toDouble)

    val dfFeatured = dfCustomer.withColumn("latitudeD", toDouble(dfCustomer("latitude")))
                                  .withColumn("longitudeD", toDouble(dfCustomer("longitude")))
                                      .select("name", "user_id","latitudeD","longitudeD")
                                      .cache()

    // Get the Customers who are with in 100kms
     dfFeatures.filter(_.distance <= 100)
               .foreach(line => println(s"Name : ${line.name} and UserID: ${line.user_id} "))
*/
  }

}
