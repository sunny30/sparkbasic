import org.apache.spark.scheduler.{JobSucceeded, SparkListener, SparkListenerApplicationEnd, SparkListenerApplicationStart, SparkListenerEvent, SparkListenerJobEnd, SparkListenerJobStart, SparkListenerStageCompleted, SparkListenerTaskEnd}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.ui.{SparkListenerDriverAccumUpdates, SparkListenerSQLExecutionEnd, SparkListenerSQLExecutionStart}
import org.apache.spark.{SparkConf, SparkContext}
import play.api.libs.json
import play.api.libs.json.{JsArray, JsBoolean, JsNumber, JsObject, JsString, JsValue, Json}
import sun.security.ec.point.ProjectivePoint.Immutable

import scala.collection.mutable.ListBuffer

object LogicalPlanTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Logical Plan serializer1").set("spark.ui.port", "6000")
    val sc = new SparkContext(conf)
    sc.addSparkListener(new ProphecySQLListener)
    val keyValues2 = Seq(("two", 1), ("three", 2), ("four", 3), ("five", 5))
    val sqlContext = new SQLContext(sc)
    val dsf = sqlContext.createDataFrame(keyValues2).toDF("k1", "v1")
    val dsf1 = sqlContext.createDataFrame(keyValues2).toDF("k1", "v2")
    val uniondf = dsf.union(dsf1)
    uniondf.show()
    //LogicalPlanSerializer(uniondf.queryExecution.analyzed,"/tmp/interim1").serializeAtGivenPath()
    // print {LogicalPlanDeserializer.apply("/tmp/interim").deserilizeFromPath().prettyJson}


  }


  class ProphecySQLListener extends SparkListener {

    var stagestoTaskMap: scala.collection.mutable.Map[Int, ListBuffer[JsObject]] = scala.collection.mutable.Map[Int, ListBuffer[JsObject]]()
    var jobsIdToJobJs:scala.collection.mutable.Map[Int,JsObject]  = scala.collection.mutable.Map[Int,JsObject]()
    var stages = ListBuffer[JsObject]()
    var jobJsObject:JsObject = JsObject.empty
    var jobs = ListBuffer[JsObject]()
    var applicationJsObject:JsObject = JsObject.empty

    override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
      case e: SparkListenerSQLExecutionStart => onExecutionStart(e)
      case e: SparkListenerSQLExecutionEnd => onExecutionEnd(e)
      case e: SparkListenerDriverAccumUpdates => onDriverAccumUpdates(e)
      case _ => // Ignore
    }

    override def onApplicationStart(applicationStart: SparkListenerApplicationStart):Unit={

      val applicationAttrMap = Map(
        "application_id"->applicationStart.appId.get,
        "user"->applicationStart.sparkUser,
        "app_name"->applicationStart.appName
      )
      applicationJsObject = Json.toJson(applicationAttrMap).as[JsObject]
    }

    override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
      val jobJsArray = jobsIdToJobJs.values.toList.foldLeft(JsArray())((acc, x) => acc ++ Json.arr(x))
      val jobsArrayJsObject = Json.toJson(Map(("jobs"->jobJsArray))).as[JsObject]
      applicationJsObject = applicationJsObject++jobsArrayJsObject
      println(applicationJsObject)
    }

    override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
      val stageJSArray = stages.foldLeft(JsArray())((acc, x) => acc ++ Json.arr(x))
      val stageJsonObject = Json.toJson(Map("stages"->stageJSArray)).as[JsObject]
      jobJsObject = jobs.find(x=>x.value.get("job_id").get.as[Int]==jobEnd.jobId).getOrElse(JsObject.empty)

      jobEnd.jobResult match {
        case JobSucceeded => {jobJsObject = jobJsObject++stageJsonObject}
        case  _ => jobJsObject=JsObject.empty
      }
      println(Json.prettyPrint(jobJsObject))
      jobsIdToJobJs = jobsIdToJobJs + (jobEnd.jobId->jobJsObject)
    }

    def onExecutionStart(event: SparkListenerSQLExecutionStart): Unit = {
      // Find the QueryExecution for the Dataset action that triggered the event
      // This is the SQL-specific way
      import org.apache.spark.sql.execution.SQLExecution
      val queryExecution = SQLExecution.getQueryExecution(event.executionId)
    }




    override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
      // Find the QueryExecution for the Dataset action that triggered the event
      // This is a general Spark Core way using local properties
//      import org.apache.spark.sql.execution.SQLExecution
//      val executionIdStr = jobStart.properties.getProperty(SQLExecution.EXECUTION_ID_KEY)
//      // Note that the Spark job may or may not be a part of a structured query
//      // jobStart.stageInfos.map(i=>i.)
//      if (executionIdStr != null) {
//        val queryExecution = SQLExecution.getQueryExecution(executionIdStr.toLong)
//        queryExecution.executedPlan
//
//      }
       jobs.insert(jobs.size,collectMetrics(jobStart))
    }


    def onExecutionEnd(event: SparkListenerSQLExecutionEnd): Unit = {}

    def onDriverAccumUpdates(event: SparkListenerDriverAccumUpdates): Unit = {}

    override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
      val stageSpecificJson= Json.toJson(collectStageMetric(stageCompleted)).as[JsObject]
      val taskArray = stagestoTaskMap.get(stageCompleted.stageInfo.stageId).get.foldLeft(JsArray())((acc, x) => acc ++ Json.arr(x))
      val completeStageInfo = (stageSpecificJson++Json.toJson(Map("tasks"->taskArray)).as[JsObject])
      stages.insert(stages.size,completeStageInfo)
      //println(Json.prettyPrint(completeStageInfo))

    }

    override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
      addTaskToStage(collectTaskMetrics(taskEnd))
    }


    def collectMetrics(job: SparkListenerJobStart): JsObject = {
      val metricMap = Map(
        "job_id" -> JsNumber(job.jobId),
        "number_of_stages" -> JsNumber(job.stageInfos.size),
        "number_of_tasks" -> JsNumber((job.stageInfos.map(i => i.numTasks).toList).sum)
      )
      Json.toJson(metricMap).as[JsObject]
    }


    def collectStageMetric(stageCompleted: SparkListenerStageCompleted): Map[String, JsValue] = {
      Map(
        ("stage_id" -> JsNumber(stageCompleted.stageInfo.stageId)),
        ("number_of_tasks" -> JsNumber(stageCompleted.stageInfo.numTasks)),
        ("stage_completion_time" -> JsNumber(stageCompleted.stageInfo.completionTime.get)),
        ("result_serialization" -> JsNumber(stageCompleted.stageInfo.taskMetrics.resultSize)),
        ("jvm_gc_time" -> JsNumber(stageCompleted.stageInfo.taskMetrics.jvmGCTime)),
        ("peak_executed_memory" -> JsNumber(stageCompleted.stageInfo.taskMetrics.peakExecutionMemory)),
        ("number_of_block_updated_status" -> JsNumber(stageCompleted.stageInfo.taskMetrics.updatedBlockStatuses.size)),
        ("memory_byte_spilled" -> JsNumber(stageCompleted.stageInfo.taskMetrics.memoryBytesSpilled)),
        ("disk_byte_spilled" -> JsNumber(stageCompleted.stageInfo.taskMetrics.diskBytesSpilled)),
        ("shuffle_fetch_wait_time" -> JsNumber(stageCompleted.stageInfo.taskMetrics.shuffleReadMetrics.fetchWaitTime)),
        ("shuffle_total_blocks_fetched" -> JsNumber(stageCompleted.stageInfo.taskMetrics.shuffleReadMetrics.totalBlocksFetched)),
        ("shuffle_total_bytes_read" -> JsNumber(stageCompleted.stageInfo.taskMetrics.shuffleReadMetrics.totalBytesRead)),
        ("shuffle_remote_blocks_fetched" -> JsNumber(stageCompleted.stageInfo.taskMetrics.shuffleReadMetrics.remoteBlocksFetched)),
        ("shuffle_write_time" -> JsNumber(stageCompleted.stageInfo.taskMetrics.shuffleWriteMetrics.writeTime)),
        ("shuffle_byte_written" -> JsNumber(stageCompleted.stageInfo.taskMetrics.shuffleWriteMetrics.bytesWritten)),
        ("shuffle_record_written" -> JsNumber(stageCompleted.stageInfo.taskMetrics.shuffleWriteMetrics.recordsWritten))
      )


    }

    def collectTaskMetrics(taskEnd: SparkListenerTaskEnd): Map[Int, JsObject]={
      val taskMetric = Map(
        ("task_id" -> JsNumber(taskEnd.taskInfo.taskId)),
        ("launch_time" -> JsNumber(taskEnd.taskInfo.launchTime)),
        ("finish_time" -> JsNumber(taskEnd.taskInfo.finishTime)),
        ("speculative" -> JsBoolean(taskEnd.taskInfo.speculative)),
        ("duration" -> JsNumber(taskEnd.taskInfo.duration)),
        ("task_locality_index" -> JsNumber(taskEnd.taskInfo.taskLocality.id)),
        ("executor_id" -> JsString(taskEnd.taskInfo.executorId))
      )
      Map(
        taskEnd.stageId -> Json.toJson(taskMetric).as[JsObject]
      )
    }

    def addTaskToStage(taskMetric: Map[Int, JsObject]): Unit = {
      taskMetric.keys.map(key => {
        if (stagestoTaskMap.contains(key)) {
          (stagestoTaskMap.get(key).get)+= taskMetric.get(key).get
        }
        else {
          stagestoTaskMap = stagestoTaskMap + (key -> ListBuffer(taskMetric.get(key).get))
        }
      }
      )
    }

  }


}
