package tungsten

import org.apache.spark.sql.execution.{GenerateExec, WholeStageCodegenExec}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._
object SparkTungsten {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("tungsten join").master("local[*]").getOrCreate()
    val sc = spark.sparkContext

    val e = expr("""trim("in3.OfferId")""")&&expr("""in3.CatalogName RLIKE concat('%', in5.GeographicRegion)""")

    concat()
    val rdd = sc.parallelize(
      Seq(
        Row(1,"fist"),
        Row(2,"second"),
        Row(3,"third")
      ))

    val schema = StructType(
      Array(
        StructField("id",IntegerType),
        StructField("name",StringType)
      )
    )

    val df = spark.createDataFrame(rdd,schema)
    val filterDf = df.filter(col("id")<2||col("name")===lit("first"))

    filterDf.show()

    val plan = filterDf.queryExecution.executedPlan
    println(plan.prettyJson)
    if(plan.isInstanceOf[WholeStageCodegenExec]){
      val ge = plan.asInstanceOf[WholeStageCodegenExec]
      val codeTuple = ge.doCodeGen()
      import org.apache.spark.sql.catalyst.expressions.codegen.CodeFormatter
      println(CodeFormatter.format(codeTuple._2))
    }else{
      val ge = plan.asInstanceOf[GenerateExec]
      val rdd = ge.execute()

    }
  }
}
