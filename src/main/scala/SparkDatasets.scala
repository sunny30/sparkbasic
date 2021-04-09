import join.SparkJoin.PM
import org.apache.spark.sql.{Dataset, Encoders, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object SparkDatasets {

  case class PM(name: String, age: Int)


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Logical Plan serializer").set("spark.ui.port","6000")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().master("local[*]").appName("sparkcolexpression").getOrCreate()

    import spark.implicits._
    val df = sc.parallelize(1 to 100).toDF("id")

    //implicit val intEncoder = Encoders.scalaInt
    val numberDS : Dataset[Int] = df.as[Int]

    val leftDf = Seq(
      PM("Sharad", 33),
      PM("Raj", 42),
      PM("Rajat", 31)
    ).toDF()

    val leftDS = leftDf.as[PM]
    leftDS.filter(_.name=="Sharad").show()

  }

}
