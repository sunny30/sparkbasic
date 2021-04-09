
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, CaseWhen, Expression, Literal, TruncTimestamp}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable

object sparkdf {

  case class PM(name: String, age: Int)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("sparkdf").getOrCreate()
    val carDf:DataFrame = spark.read.format("json").option("inferSchema",true).load("/Users/sharadsingh/Dev/scalaSparkEx/src/main/scala/sparkdataframe/Cars.json")
     carDf.show()
    //  carDf.printSchema()
    val carsSchema = getCarSchema
    //read a datafrme with your own defined schema
    //  val carWithSchema = spark.read.format("json").schema(carDf.schema).load("/Users/sharadsingh/Dev/scalaSparkEx/src/main/scala/sparkdataframe/Cars.json")

    val e = expr("CASE WHEN (XID IS NULL) THEN CAST(IID AS STRING) ELSE CAST(XID AS STRING) END")
    val sql = "CASE WHEN (XID IS NULL) THEN CAST(IID AS STRING) ELSE CAST(XID AS STRING) END"
    val csql = "CASE WHEN (ust.`UserTypeId` IS NULL) THEN 0 ELSE ust.`UserTypeId` END"
    val ccsql = "CatalogName RLIKE concat(lit(\"%\"), GeographicRegion)"
//    val cs = expr(ccsql)
//    caseWhenSqlString(cs.expr.asInstanceOf[CaseWhen])

    val nsql = sql.replace("XID","in0.XID")
    println(nsql)
    (5 to 0 by(-1)).foreach(println)
    //getScalaString(e.expr)
    import spark.implicits._
    val pmDf = Seq(
      PM("Sharad", 33),
      PM("Raj", 42),
      PM("Rajat", 31)
    ).toDF()

   // pmDf("name").rlike(concat(lit('%'),"""))

    pmDf.printSchema()

    val tupleDF = Seq(
      ("Sunny", 33),
      ("rb", 42)
    ).toDF("name", "age")

    val newDF = tupleDF.select(when(tupleDF("name") === "Raj", 0)
      .when(tupleDF("name") === "Sharad", 1)
      .otherwise(2))
    newDF.queryExecution.optimizedPlan

    tupleDF.schema

  }


  def getCarSchema: StructType = {
    val schema = StructType(
      Array(
        StructField("Name", StringType),
        StructField("Miles_per_gallon", IntegerType),
        StructField("Cylinders", IntegerType),
        StructField("Displacement", IntegerType),
        StructField("HorsePower", IntegerType),
        StructField("Weight_in_lbs", IntegerType),
        StructField("Acceleration", DoubleType),
        StructField("Year", StringType),
        StructField("Origin", StringType)
      )
    )
    schema
  }

  def getScalaString(expression: Expression): String = {

    if (expression.isInstanceOf[CaseWhen]) {
      val ex = expression.asInstanceOf[CaseWhen]
      ex.branches
      ""
    }
    "hel"
  }

  def caseWhenSqlString(expression: CaseWhen): String = {
    val columnMap = mutable.Map.empty[String, String]
    expression.branches.map { x ⇒
      val apair = attributePair(x._1)
      val bpair = attributePair(x._2)
      columnMap += (apair._1 → apair._2)
      columnMap += (bpair._1 → bpair._2)
    }

    var sqlString = expression.sql
    columnMap.map { x ⇒
      sqlString = sqlString.replace(" " + x._1, " " + x._2)
      sqlString = sqlString.replace("(" + x._1, "(" + x._2)
    }
    sqlString

  }

  def attributePair(expression: Expression): (String, String) = {
    if (expression != null && (expression.isInstanceOf[AttributeReference] || expression.isInstanceOf[UnresolvedAttribute] || expression.isInstanceOf[Literal])) {
      if(expression.isInstanceOf[AttributeReference]) {
        val attr = expression.asInstanceOf[AttributeReference]
        (attr.name, attr.sql)
      }else if(expression.isInstanceOf[UnresolvedAttribute]) {
        val attr = expression.asInstanceOf[UnresolvedAttribute]
        (attr.name, attr.name)
      }else{
        val attr = expression.asInstanceOf[Literal]
        (attr.sql, attr.sql)
      }
    } else {
      attributePair(expression.children(0))

    }
  }

  def getDateTruncExprString(expression:TruncTimestamp):String={
    val columnMap = mutable.Map.empty[String,String]
    expression.children.map(x=>{
      if(x.isInstanceOf[AttributeReference]){
        columnMap+=(x.sql->x.sql)
      }
    })
    var sqlString = expression.sql
    columnMap.map(x=>{
      sqlString = sqlString.replace(x._1 ,x._2)
    })
    sqlString

  }

}
