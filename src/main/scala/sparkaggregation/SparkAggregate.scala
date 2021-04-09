package sparkaggregation

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object SparkAggregate {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("sparkcolexpression").getOrCreate()
    val moviesDf: DataFrame = spark.read.format("json").option("inferSchema", true).
      load("/Users/sharadsingh/Dev/scalaSparkEx/src/main/scala/sparkcolumnsexpressions/movies.json")

    moviesDf.select(countDistinct(col("Major_Genre")).as("count_unique_genre")).show()
    moviesDf.select(col("Major_Genre").as("Unique_genre")).distinct().show()

    val groupByDf = moviesDf.groupBy(col("Major_Genre"))
    val out = groupByDf.agg(count("Title"))
    out.show()

    val groupByDF1 = moviesDf.groupBy(col("Major_Genre"))
    val out1= groupByDf.agg(
      count("*").as("NMovies"),
      avg("IMDB_Rating").as("avg_rating")
    ).show()

  }
}
