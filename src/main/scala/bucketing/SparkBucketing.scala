package bucketing

import org.apache.spark.sql.SparkSession

object SparkBucketing {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("bucketingApp").getOrCreate()
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)


    val ldf = spark.range(10000)
    val rdf = spark.range(100)

    ldf.write.bucketBy(4,"id").saveAsTable("largetable")
    rdf.write.bucketBy(4,"id").saveAsTable("smalltable")


    val largeDf = spark.table("largetable")
    val smallDf = spark.table("smalltable")

    largeDf.join(smallDf,Seq("id"))
    val out = largeDf.join(smallDf,largeDf.col("id")===smallDf.col("id"))
    out.explain()
    /*
    == Physical Plan ==
*(3) SortMergeJoin [id#6L], [id#8L], Inner
:- *(1) Sort [id#6L ASC NULLS FIRST], false, 0
:  +- *(1) Project [id#6L]
:     +- *(1) Filter isnotnull(id#6L)
:        +- *(1) ColumnarToRow
:           +- FileScan parquet default.largetable[id#6L] Batched: true, DataFilters: [isnotnull(id#6L)], Format: Parquet, Location: InMemoryFileIndex[file:/Users/sharadsingh/Dev/scalaSparkEx/spark-warehouse/largetable], PartitionFilters: [], PushedFilters: [IsNotNull(id)], ReadSchema: struct<id:bigint>, SelectedBucketsCount: 4 out of 4
+- *(2) Sort [id#8L ASC NULLS FIRST], false, 0
   +- *(2) Project [id#8L]
      +- *(2) Filter isnotnull(id#8L)
         +- *(2) ColumnarToRow
            +- FileScan parquet default.smalltable[id#8L] Batched: true, DataFilters: [isnotnull(id#8L)], Format: Parquet, Location: InMemoryFileIndex[file:/Users/sharadsingh/Dev/scalaSparkEx/spark-warehouse/smalltable], PartitionFilters: [], PushedFilters: [IsNotNull(id)], ReadSchema: struct<id:bigint>, SelectedBucketsCount: 4 out of 4

     */
  }

}
