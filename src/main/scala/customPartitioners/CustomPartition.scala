package customPartitioners

import org.apache.spark.{HashPartitioner, RangePartitioner}
import org.apache.spark.sql.SparkSession

object CustomPartition {


  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("tungsten join").master("local[*]").getOrCreate()
    val sc = spark.sparkContext

    val numbers = sc.parallelize(1 to 10000)
    println(numbers.partitioner)

    val tupleNumbers = numbers.map(n=>(n%10,n))
    val hashNumbers= tupleNumbers.partitionBy(new HashPartitioner(4))
    val rangedPartitionBy = tupleNumbers.partitionBy(new RangePartitioner(4,tupleNumbers))
  }
}
