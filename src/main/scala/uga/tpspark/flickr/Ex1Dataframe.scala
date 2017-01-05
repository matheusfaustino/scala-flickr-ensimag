package uga.tpspark.flickr

import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.ByteType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.LongType
import java.net.URLDecoder
import org.apache.spark.sql.Row
import org.apache.spark.sql.Encoders

object Ex1Dataframe {
  def main(args: Array[String]): Unit = {
    println("hello")
    var spark: SparkSession = null
    try {
      spark = SparkSession.builder().appName("Flickr using dataframes").getOrCreate()

      //   * Photo/video identifier
      //   * User NSID
      //   * User nickname
      //   * Date taken
      //   * Date uploaded
      //   * Capture device
      //   * Title
      //   * Description
      //   * User tags (comma-separated)
      //   * Machine tags (comma-separated)
      //   * Longitude
      //   * Latitude
      //   * Accuracy
      //   * Photo/video page URL
      //   * Photo/video download URL
      //   * License name
      //   * License URL
      //   * Photo/video server identifier
      //   * Photo/video farm identifier
      //   * Photo/video secret
      //   * Photo/video secret original
      //   * Photo/video extension original
      //   * Photos/video marker (0 = photo, 1 = video)

      val customSchemaFlickrMeta = StructType(Array(
        StructField("photo_id", LongType, true),
        StructField("user_id", StringType, true),
        StructField("user_nickname", StringType, true),
        StructField("date_taken", StringType, true),
        StructField("date_uploaded", StringType, true),
        StructField("device", StringType, true),
        StructField("title", StringType, true),
        StructField("description", StringType, true),
        StructField("user_tags", StringType, true),
        StructField("machine_tags", StringType, true),
        StructField("longitude", FloatType, false),
        StructField("latitude", FloatType, false),
        StructField("accuracy", StringType, true),
        StructField("url", StringType, true),
        StructField("download_url", StringType, true),
        StructField("license", StringType, true),
        StructField("license_url", StringType, true),
        StructField("server_id", StringType, true),
        StructField("farm_id", StringType, true),
        StructField("secret", StringType, true),
        StructField("secret_original", StringType, true),
        StructField("extension_original", StringType, true),
        StructField("marker", ByteType, true)))
        
      val customSchemaFlickrLicense = StructType(Array(
          StructField("name", StringType, true),
          StructField("non_commercial", ByteType, true),
          StructField("non_derivative", ByteType, true),
          StructField("share_alike", ByteType, true),
          StructField("public_domain_dedication", ByteType, true),
          StructField("public_domain_work", ByteType, true)))

      val originalFlickrMeta = spark.sqlContext.read
        .format("csv")
        .option("delimiter", "\t")
        .option("header", "false")
        .schema(customSchemaFlickrMeta)
        .load("flickrSample.txt")
        
      val originalFlickLicense = spark.sqlContext.read
        .format("csv")
        .option("delimiter", "\t")
        .option("header", "true")
        .schema(customSchemaFlickrLicense)
        .load("FlickrLicense.txt")
        
//      val premier_truc = originalFlickrMeta.select("photo_id", "longitude", "latitude", "license");
//      premier_truc.show()
//      premier_truc.explain()
      val interessantes = originalFlickrMeta
          .join(originalFlickLicense, originalFlickLicense("name") === originalFlickrMeta("license"))
          .where("latitude <> -1.0")
          .where("longitude <> -1.0")
          .where("license IS NOT NULL")
          .where(originalFlickLicense("name").toString() + " LIKE \"%NoDerivs%\"")
          .cache()
          
      interessantes.show()
      
      /* export data */
      interessantes.write
                   .format("com.databricks.spark.csv")
                   .option("header", "true")
                   .save("myfirstfile.csv")
      
      /* Using Cache */
//      val copyInter = originalFlickrMeta
//          .join(originalFlickLicense, originalFlickLicense("name") === originalFlickrMeta("license"))
//          .where("latitude <> -1.0")
//          .where("longitude <> -1.0")
//          .where("license IS NOT NULL")
//          .where(originalFlickLicense("name").toString() + " LIKE \"%NoDerivs%\"");
//      
//      copyInter.show()
      

      
    } catch {
      case e: Exception => throw e
    } finally {
      spark.stop()
    }
    println("done")
  }
}