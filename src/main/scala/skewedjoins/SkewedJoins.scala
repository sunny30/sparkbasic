package skewedjoins

import org.apache.spark.sql.SparkSession

object SkewedJoins {


  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("bucketingApp").getOrCreate()
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

  }
}
