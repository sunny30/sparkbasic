import com.esotericsoftware.kryo.io.Input
import org.apache.hadoop.io.{BytesWritable, NullWritable}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import scala.reflect.runtime.universe.TypeTag


import scala.reflect.ClassTag

//case class LogicalPlanRDD(val logicalPlan: LogicalPlan)


//class LogicalPlanDeserializer(path:String) {
//
//  def deserilizeFromPath():LogicalPlan={
//
//    val conf = new SparkConf().setMaster("local[*]").setAppName("Logical Plan serializer").set("spark.driver.allowMultipleContexts","true")
//    val sc = new SparkContext(conf)
//    val deserializedRDD = objectFile[LogicalPlanRDD](sc, path)
//    val logicalPlan = deserializedRDD.collect()(0).logicalPlan
//    logicalPlan
//  }
//
//  def objectFile[A : ClassTag](sc : SparkContext, path : String, minPartitions: Int = 1)(implicit tt : TypeTag[A]) = {
//    val kryoSerializer = new KryoSerializer(sc.getConf)
//
//    sc.sequenceFile(path, classOf[NullWritable], classOf[BytesWritable], minPartitions)
//      .flatMap(x => {
//        val kryo = kryoSerializer.newKryo()
//        val input = new Input()
//        input.setBuffer(x._2.getBytes)
//        val data = kryo.readClassAndObject(input)
//        val dataObject = data.asInstanceOf[Array[A]]
//        dataObject
//      })
//  }
//}
//
//object LogicalPlanDeserializer{
//  def apply(path: String): LogicalPlanDeserializer = new LogicalPlanDeserializer(path)
//}


