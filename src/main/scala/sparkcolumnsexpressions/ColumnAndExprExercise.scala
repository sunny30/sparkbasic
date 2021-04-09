package sparkcolumnsexpressions

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object ColumnAndExprExercise {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("sparkcolexpression").getOrCreate()
    val moviesDf:DataFrame = spark.read.format("json").option("inferSchema",true).
      load("/Users/sharadsingh/Dev/scalaSparkEx/src/main/scala/sparkcolumnsexpressions/movies.json")

    val gross_df = grossProfit(spark,moviesDf)
    gross_df.show(10)

    val mp = Map(
      ("Major_Genre"->("Comedy","=","String")),
      ("IMDB_Rating")->("6.2",">","Long")
    )

    val filterDf = filterMovies(spark,moviesDf,mp)
    filterDf.show(10)


  }

  def grossProfit(sparkSession: SparkSession,in:DataFrame):DataFrame={

    val out = in.na.fill(0,Seq("US_DVD_Sales")).withColumn("gross_profit",col("US_Gross")+col("Worldwide_Gross")+col("US_DVD_Sales"))
    out.select(col("Title"),col("gross_profit"))

  }

  def filterMovies(sparkSession: SparkSession,in:DataFrame,map: Map[String,(String,String,String)]):DataFrame={
      val in1 = in.as("in0")
      val condString = map.map(x=>{
        if(x._2._3.equals("String"))
          s"""in0.${x._1}${x._2._2}\"${x._2._1}\""""
        else{
          s"""in0.${x._1}${x._2._2}${x._2._1}"""
        }
        }

      ).mkString(" AND ")
    in1.filter(expr(condString)).select(col("Title"),col("IMDB_Rating"))
  }



}
