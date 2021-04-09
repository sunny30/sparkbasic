package join

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.{CostMode, WholeStageCodegenExec}
import org.apache.spark.sql.functions._
object SparkJoin {

  case class PM(name: String, age: Int)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[*]").appName("sparkcolexpression").getOrCreate()

    import spark.implicits._
    val leftDf = Seq(
      PM("Sharad", 33),
      PM("Raj", 42),
      PM("Rajat", 31)
    ).toDF()

    val rightDF = Seq(
      PM("Sharad", 33),
      PM("Raj1", 42),
      PM("Rajat1", 31)
    ).toDF()

    val joinDf = leftDf.join(rightDF, leftDf.col("name")===rightDF.col("name"),"left_outer")

    val groupby = joinDf.groupBy(leftDf("age"))
    val aggDf = groupby.agg(sum(leftDf("name")))
    //joinDf.show()
    //println(joinDf.queryExecution.explainString(CostMode))
   println(getStageId(aggDf,spark))

    leftDf.select(sum(col("age"))).explain(true)
  }


  def getStageId(df:DataFrame,spark:SparkSession):Int={
    val plan = df.queryExecution.sparkPlan
    import org.apache.spark.sql.execution.exchange.EnsureRequirements
    val erRule = EnsureRequirements(spark.sessionState.conf)



    val planAfterER = erRule.apply(plan)
    import org.apache.spark.sql.execution.CollapseCodegenStages
    val ccsRule = CollapseCodegenStages(spark.sessionState.conf)
    val planAfterCCS = ccsRule.apply(planAfterER)
    //planAfterCCS
    planAfterCCS.asInstanceOf[WholeStageCodegenExec].codegenStageId
  }
}
