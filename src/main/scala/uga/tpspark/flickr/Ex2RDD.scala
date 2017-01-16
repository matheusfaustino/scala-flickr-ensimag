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
import org.apache.spark.rdd.RDD
import scala.tools.nsc.transform.Flatten
import scala.collection.TraversableOnce
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec._

object Ex2RDD {
  def main(args: Array[String]): Unit = {
    println("hello")
    var spark: SparkSession = null
    try {
      spark = SparkSession.builder().appName("Flickr using dataframes").getOrCreate()
      val originalFlickrMeta: RDD[String] = spark.sparkContext.textFile("flickrSample.txt")
      
      // exec 1
      println("exec 1")
      originalFlickrMeta
        .take(5)
        .map(i => println(i))
      println(originalFlickrMeta.count())
      
      // exec 2
      println("exec 2")
      val goodPic: RDD[Picture] = originalFlickrMeta
                                    .map(i => i.split("\\t"))
                                    .map(i => new Picture(i))
                                    .filter(i => i.hasValidCountry && i.hasTags)
      goodPic.take(5).map(println);
                                    
      // exec 3
      println("exec 3")
      val perPays: RDD[(Country, Iterable[Array[String]])] = goodPic.groupBy(_.c)
                                                             .map{ 
                                                                  kv => (
                                                                      kv._1, 
                                                                      kv._2.map(_.userTags)
                                                                 ) 
                                                             };
      perPays.take(1).map(e => { 
         println(e._1); 
//         e._2.map(i => i.map(println))
         e._2.map(println)
      });
      
      // exec 4
      println("exec 4")
      val flattenPerPays = perPays.map(i => {
        (i._1, i._2.flatten)
      })
      flattenPerPays.take(1).map(println);
      
      // exec 5
      println("exec 5")
      val sansRepetitions: RDD[(Country, Map[String, Int])] = flattenPerPays.map(i => {
          (
              i._1, 
              i._2.groupBy(identity)
                  .map{ kv => (kv._1, kv._2.count(_ == kv._1) ) }
         )
      })
      sansRepetitions.take(1).map(println);
      
    } catch {
      case e: Exception => throw e
    } finally {
      spark.stop()
    }
    println("done")
  }
}