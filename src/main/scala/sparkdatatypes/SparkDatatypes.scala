package sparkdatatypes

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._


object SparkDatatypes {


  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[*]").appName("sparkcolexpression").getOrCreate()
    val moviesDf: DataFrame = spark.read.format("json").option("inferSchema", true).
      load("/Users/sharadsingh/Dev/scalaSparkEx/src/main/scala/sparkcolumnsexpressions/movies.json")

    val firstPredicate = col("Major_Genre") === lit("Drama")
    val goodMoviePredicate = col("IMDB_Rating")>lit(.7)
    moviesDf.filter(firstPredicate).select("Major_Genre")show()

    val condPredicate = firstPredicate and goodMoviePredicate
    moviesDf.select(col("Title"),condPredicate.as("is_good_movie")).show()

  }

}
