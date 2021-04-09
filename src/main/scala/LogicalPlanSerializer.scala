import java.io.ByteArrayOutputStream

import org.apache.hadoop.io.{BytesWritable, NullWritable}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import scala.reflect.ClassTag

//case class LogicalPlanRDD(val logicalPlan: LogicalPlan)


//class LogicalPlanSerializer(logicalPlan:LogicalPlan,path:String) {
//
//  def serializeAtGivenPath():Unit={
//    val conf = new SparkConf().setMaster("local[*]").
//      setAppName("Logical Plan serializer").set("spark.driver.allowMultipleContexts","true")
//    val sc = new SparkContext(conf)
//    val data = Array(logicalPlan)
//    val rdd = sc.parallelize(data).map[LogicalPlanRDD](x => LogicalPlanRDD(x))
//    saveAsObjectFile[LogicalPlanRDD](rdd,path)
//  }
//
//  private def saveAsObjectFile[A : ClassTag](rdd : RDD[A], path : String): Unit = {
//    val kryoSerializer = new KryoSerializer(rdd.context.getConf)
//
//    rdd.mapPartitions(iter => iter.grouped(10)
//      .map(_.toArray))
//      .map(splitArray => {
//        val kryo = kryoSerializer.newKryo()
//
//        val bao = new ByteArrayOutputStream()
//        val output = kryoSerializer.newKryoOutput()
//        output.setOutputStream(bao)
//        kryo.writeClassAndObject(output, splitArray)
//        output.close()
//
//        val byteWritable = new BytesWritable(bao.toByteArray)
//        (NullWritable.get(), byteWritable)
//      }).saveAsSequenceFile(path)
//  }
//
//}
//object LogicalPlanSerializer{
//  def apply(logicalPlan: LogicalPlan, path: String): LogicalPlanSerializer = new LogicalPlanSerializer(logicalPlan, path)
//}
//



