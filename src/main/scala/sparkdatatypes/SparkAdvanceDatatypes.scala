package sparkdatatypes

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
object SparkAdvanceDatatypes {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[*]").appName("sparkcolexpression").getOrCreate()
    val moviesDf: DataFrame = spark.read.format("json").option("inferSchema", true).
      load("/Users/sharadsingh/Dev/scalaSparkEx/src/main/scala/sparkcolumnsexpressions/movies.json")

    spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")
    moviesDf.select(col("Title"),to_date(col("Release_date"),"dd-MMM-yy"),to_date(lit("23-10-2001"),"dd-MM-yyyy").as("diff_format")).show()

    moviesDf.select(col("Title"),struct(col("IMDB_Rating"),col("Rotten_Tomatoes_Rating")).as("Ratings")).
      select(col("Title"),col("Ratings").getField("Rotten_Tomatoes_Rating")).show()

    val cl = coalesce(moviesDf.col("Rotten_Tomatoes_Rating"),moviesDf.col("IMDB_Rating"))
    println(cl.expr.sql)
  }

  }
