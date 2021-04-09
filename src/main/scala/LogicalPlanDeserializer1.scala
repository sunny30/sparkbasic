import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag


class LogicalPlanDeserializer1(path: String) {

  def deserialize(sc: SparkContext): LogicalPlan = {
    val deserializedRDD = objectFile[LogicalPlanRDD](sc, path)
    val logicalPlan     = deserializedRDD.collect()(0).logicalPlan
    logicalPlan
  }

  private def objectFile[A: ClassTag](sc: SparkContext, path: String)(implicit tt: TypeTag[A]) =
    sc.objectFile(path).map { x ⇒
      x.asInstanceOf[A]
    }
}

object LogicalPlanDeserializer1 {
  def apply(path: String): LogicalPlanDeserializer1 = new LogicalPlanDeserializer1(path)
}