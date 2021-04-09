import LogicalPlanTest.ProphecySQLListener
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{LocalTableScanExec, SparkPlan, WholeStageCodegenExec}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}

object StageMapping {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Logical Plan serializer1").set("spark.ui.port", "6000")
    val sc = new SparkContext(conf)
    sc.addSparkListener(new ProphecySQLListener)
    val keyValues2 = Seq(("two", 1), ("three", 2), ("four", 3), ("five", 5))
    val sqlContext = new SQLContext(sc)
    //sqlContext.sparkSession.conf.set("spark.sql.join.preferSortMergeJoin", false)
   // sqlContext.sparkSession.conf.set("spark.sql.autoBroadcastJoinThreshold", 1)



    import sqlContext.sparkSession.implicits._
    import org.apache.spark.sql.functions.{explode,lit}

    val dsf = sqlContext.createDataFrame(keyValues2).toDF("k1", "v1")
    dsf.rdd.setName("dsf")
    println(dsf.rdd.name)
    val totalCount = dsf.rdd.glom().map(_.length).sum()
    println(totalCount)
    dsf.createOrReplaceTempView("ttable")
    sqlContext.sparkSession.emptyDataFrame.createOrReplaceTempView("ttable1")


//    sqlContext.sparkSession.sql("select k1 as k  into ttable1 from ttable ")
//    val qdf = sqlContext.sparkSession.sql("select count(distinct k1) from ttable")
//    val qdf1 = sqlContext.sparkSession.sql("update ttable set k1 = 2 where k1=2")
//    println(qdf1.queryExecution.logical.numberedTreeString)
//    val tstring = qdf.queryExecution.logical.numberedTreeString
//    println(tstring)
//    val dsf1 = sqlContext.createDataFrame(keyValues2).toDF("k1", "v2")
//    val ddf = sqlContext.sparkSession.range(10).where("id>4")
    val dataset = Seq(
      (0, "playing"),
      (1, "with"),
      (2, "ShuffledHashJoinExec")
    ).toDF("id", "token")
    val dt = dataset.select("id")
//    val stageId1 = getStageId(dt)
    // Self LEFT SEMI join
    val q = dataset.join(dataset, Seq("id"), "leftsemi")
//    val q = sqlContext.sparkSession.range(2)
//      .filter($"id" === 0)
//      .select(explode(lit(Array(0,1,2))) as "exploded")
//      .join(sqlContext.sparkSession.range(2))
//      .where($"exploded" === $"id")
   // val uniondf = dsf.union(dsf1)
    import org.apache.spark.sql.functions.{expr,col}
   // val selectDF= uniondf.select(expr("k1"),expr("v1"))
    val stageId = getStageId(q.toDF())
    stageMapping(q.toDF(),sqlContext.sparkSession)
    q.show()



  }

  def stageMapping(df:DataFrame,spark:SparkSession):Unit={

    import org.apache.spark.sql.internal.SQLConf.WHOLESTAGE_CODEGEN_ENABLED
    println(spark.conf.get("spark.sql.codegen.wholeStage"))
    val plan = df.queryExecution.sparkPlan
    import org.apache.spark.sql.execution.exchange.EnsureRequirements
    val erRule = EnsureRequirements(spark.sessionState.conf)
    val planAfterER = erRule.apply(plan)
    import org.apache.spark.sql.execution.CollapseCodegenStages
    val ccsRule = CollapseCodegenStages(spark.sessionState.conf)
    val planAfterCCS = ccsRule.apply(planAfterER)
    println(planAfterCCS.asInstanceOf[WholeStageCodegenExec].codegenStageId)
    println(planAfterCCS.numberedTreeString)

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

  def getStageId(df:DataFrame):Int={
      getStageId(df,df.sparkSession)
  }

  def getStageId(plan:LogicalPlan, spark:SparkSession):Int={

    val queryExecution = spark.sessionState.executePlan(plan)
    import org.apache.spark.sql.execution.exchange.EnsureRequirements
    val erRule = EnsureRequirements(spark.sessionState.conf)
    val planAfterER = erRule.apply(queryExecution.sparkPlan)
    import org.apache.spark.sql.execution.CollapseCodegenStages
    val ccsRule = CollapseCodegenStages(spark.sessionState.conf)
    val planAfterCCS = ccsRule.apply(planAfterER)
    planAfterCCS.asInstanceOf[WholeStageCodegenExec].codegenStageId
  }

  def getStageId(df:DataFrame,isRDD:Boolean):Int={
    import org.apache.spark.TaskContext
    val ctx = TaskContext.get()
    val stageId = ctx.stageId()
    stageId
  }

}
