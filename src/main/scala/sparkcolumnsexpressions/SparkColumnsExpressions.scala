package sparkcolumnsexpressions

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object SparkColumnsExpressions {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("sparkcolexpression").getOrCreate()
    val carDf:DataFrame = spark.read.format("json").option("inferSchema",true).
      load("/Users/sharadsingh/Dev/scalaSparkEx/src/main/scala/sparkdataframe/Cars.json")
    val accColumn = carDf.col("Acceleration")
    val accDF = carDf.select(accColumn.as("alias_column"))
    accDF.show(10)
    val reformatDF = carDf.select(col("name"),
      col("Acceleration"))//'Year,column("Horsepower")
    reformatDF.show(10)



  }

}
