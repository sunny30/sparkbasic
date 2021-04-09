package joinoptimization

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
object SparkBroadCastJoin {


  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("broadCast join").master("local[*]").getOrCreate()
    val sc = spark.sparkContext

    import spark.implicits._
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

    val largeDF = sc.parallelize(1 to 1000000).toDF("id")

    val joinDf = largeDF.join(df,Seq("id"),"inner")

    joinDf.explain()
    /*
    == Physical Plan ==
*(5) Project [id#11, name#3]
+- *(5) SortMergeJoin [id#11], [id#2], Inner
   :- *(2) Sort [id#11 ASC NULLS FIRST], false, 0
   :  +- Exchange hashpartitioning(id#11, 200), true, [id=#38]
   :     +- *(1) Project [value#8 AS id#11]
   :        +- *(1) SerializeFromObject [input[0, int, false] AS value#8]
   :           +- Scan[obj#7]
   +- *(4) Sort [id#2 ASC NULLS FIRST], false, 0
      +- Exchange hashpartitioning(id#2, 200), true, [id=#44]
         +- *(3) Filter isnotnull(id#2)
            +- *(3) Scan ExistingRDD[id#2,name#3]
     */

    val optimizeJoin = largeDF.join(broadcast(df),Seq("id"),"inner")
    //optimizeJoin.explain()
    /*
    == Physical Plan ==
*(2) Project [id#11, name#3]
+- *(2) BroadcastHashJoin [id#11], [id#2], Inner, BuildRight
   :- *(2) Project [value#8 AS id#11]
   :  +- *(2) SerializeFromObject [input[0, int, false] AS value#8]
   :     +- Scan[obj#7]
   +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint))), [id=#86]
      +- *(1) Filter isnotnull(id#2)
         +- *(1) Scan ExistingRDD[id#2,name#3]
     */


    spark.conf.set("spark.sql.autoBroadcastJoinThreshold",-1)

    val df2 = spark.createDataFrame(rdd,schema).repartition(col("id"))
    val largeDF2 = sc.parallelize(1 to 1000000).toDF("id").repartition(col("id"))
    val out = partitionOptimization(largeDF,df2,"id")
    /*
    == Physical Plan ==
*(5) Project [id#11, name#3]
+- *(5) SortMergeJoin [id#11], [id#2], Inner
   :- *(2) Sort [id#11 ASC NULLS FIRST], false, 0
   :  +- Exchange hashpartitioning(id#11, 200), false, [id=#44]
   :     +- *(1) Project [value#8 AS id#11]
   :        +- *(1) SerializeFromObject [input[0, int, false] AS value#8]
   :           +- Scan[obj#7]
   +- *(4) Sort [id#2 ASC NULLS FIRST], false, 0
      +- Exchange hashpartitioning(id#2, 200), false, [id=#50]
         +- *(3) Filter isnotnull(id#2)
            +- *(3) Scan ExistingRDD[id#2,name#3]
     */


  }

  def partitionOptimization(leftDf:DataFrame,rightDf:DataFrame,partitionColumnName:String):DataFrame={
   // val ldf = leftDf.repartition(col(partitionColumnName))
  //  val rdf = rightDf.repartition(col(partitionColumnName))

    val out = leftDf.join(rightDf,Seq(partitionColumnName))
    out.explain()
    out
  }
}
