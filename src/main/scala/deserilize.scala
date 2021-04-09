//import org.apache.spark.rdd.RDD
//import org.apache.spark.serializer.KryoSerializer
//import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
//import org.apache.hadoop.io.{BytesWritable, NullWritable}
//import org.apache.spark.{SparkConf, SparkContext}
//import com.esotericsoftware.kryo.io.Input
//import java.io.ByteArrayOutputStream
//import scala.reflect.runtime.universe.TypeTag
//
//
//
//
//import scala.reflect.ClassTag
//
//object deserilize {
//
//  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf().setMaster("local[*]").setAppName("Logical Plan serializer")
//    val sc = new SparkContext(conf)
//    val path = "/Users/sharadsingh/Dev/scalaLearning/rdd3"
//   val logicalPlan =  LogicalPlanDeserializer(path).deserilizeFromPath()
////    val deserializedRDD = KryoSerializerDeserializer().objectFile[LogicalPlanRDD](sc, path)
////    val logicalPlan = deserializedRDD.collect()(0).logicalPlan
//    println(logicalPlan.prettyJson)
//  }
//
//}
//
//
//class KryoSerializerDeserializer1 {
//  def saveAsObjectFile[A : ClassTag](rdd : RDD[A], path : String): Unit = {
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
//object KryoSerializerDeserializer1 {
//  def apply() = new KryoSerializerDeserializer1
//}
