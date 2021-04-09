
import java.io.ByteArrayOutputStream

import com.esotericsoftware.kryo.io.Input
import org.apache.hadoop.io.{BytesWritable, NullWritable}
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Alias, And, AttributeReference, EqualNullSafe, EqualTo, Expression, IsNotNull, Literal}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Sum}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.runtime.universe.TypeTag
import scala.reflect.ClassTag


case class LogicalPlanRDD(val logicalPlan: LogicalPlan)

object MainApplication extends B2CommonUDFs {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Logical Plan serializer").set("spark.ui.port","6000")
    val sc = new SparkContext(conf)

    //val plan = LogicalPlanDeserializer1("/Users/sharadsingh/Documents/test_output").deserialize(sc)

    val sqlContext = new SQLContext(sc)

    val keyValues1 = Seq(("one", "1"), ("two", "2"), ("three", "3"), ("one", "5"))


    //val sparkSession = SparkSession.getActiveSession
    sqlContext.sparkSession.udf.register("power3df",power3(_:Double,_:String):String)
    sqlContext.createDataFrame(keyValues1).toDF("k1", "v1").createOrReplaceTempView("df1")

    val dsf = sqlContext.createDataFrame(keyValues1).toDF("k1", "v1")
    val dsf1 = sqlContext.createDataFrame(keyValues1).toDF("k1", "v2")

    val dd = dsf.union(dsf1)
    dd.queryExecution.analyzed.prettyJson


    val keyValues2 = Seq(("two", 1), ("three", 2), ("four", 3), ("five", 5))

    val left = sqlContext.createDataFrame(keyValues1).toDF("k1", "v1")
    left.createOrReplaceTempView("left")
    val right = sqlContext.createDataFrame(keyValues2).toDF("k2", "v2")
    right.createOrReplaceTempView("right")
    left.join(right,org.apache.spark.sql.functions.expr("v1").cast(IntegerType)===org.apache.spark.sql.functions.expr("v2"),"inner").show()

    sqlContext.createDataFrame(keyValues2).toDF("k2", "v2").createOrReplaceTempView("df2")

    sqlContext.sparkSession.udf.register("b2drfRejFlgUDF", b2drfRejFlg)
    sqlContext.sparkSession.udf.register("b2otcRejFlgUDF", b2OtcRejFlg)
    sqlContext.sparkSession.udf.register("cmlsJrsdctnUDF", cmlsJrsdctnUDF)
    sqlContext.sparkSession.udf.register("gmbsBinCtryCdIncomingUDF", gmbsBinCtryCdIncoming)
    sqlContext.sparkSession.udf.register("gmbsBinCtryCdIncomingOthUDF", gmbsBinCtryCdIncomingOth)
    sqlContext.sparkSession.udf.register("gmbsBinCtryCdOutgoingUDF", gmbsBinCtryCdOutgoing)
    sqlContext.sparkSession.udf.register("gmbsBinCtryCdOutgoingOthUDF", gmbsBinCtryCdOutgoingOth)
    sqlContext.sparkSession.udf.register("gmbsBinRegnCdIncomingUDF", gmbsBinRegnCdIncoming)
    sqlContext.sparkSession.udf.register("gmbsBinRegnCdIncomingOthUDF", gmbsBinRegnCdIncomingOth)
    sqlContext.sparkSession.udf.register("gmbsBinRegnCdOutgoingUDF", gmbsBinRegnCdOutgoing)
    sqlContext.sparkSession.udf.register("gmbsBinRegnCdOutgoingOthUDF", gmbsBinRegnCdOutgoingOth)
    sqlContext.sparkSession.udf.register("gmbsBinOrigCtryCdIncomingUDF", gmbsBinOrigCtryCdIncoming)
    sqlContext.sparkSession.udf.register("gmbsBinOrigCtryCdOutgoingUDF", gmbsBinOrigCtryCdOutgoing)

    val ddf1 = sqlContext.sql("select k1,v1 from df1") ;
    val ddf2 = sqlContext.sql("select k2,v2 from df2") ;

    import org.apache.spark.sql.functions._

    ddf1.withColumn("new_col",lit("bb")).show()
    ddf1.select(expr("""k1""")).show()




    val ex9 = org.apache.spark.sql.functions.expr("'bb' as bc")
    val ex = org.apache.spark.sql.functions.expr("df1.k1=df2.k2")
    val ex2 = org.apache.spark.sql.functions.expr("""b2drfRejFlgUDF("hello","hi",1,"bye") as colN""")
    val ex3 = org.apache.spark.sql.functions.expr("""((`x.a.cmls_srce_cib` IS NOT NULL) AND (NOT (`x.a.cmls_srce_cib` = 0)))""")
    val ex4 = org.apache.spark.sql.functions.expr("""concat_ws("", substring(`cmls_cpd_dt`, 3, 4), "00")""").as("gmbs_billing_cylce_id")
    val s = ex4.expr.sql
    val sa = s"""expr(\"\"\"$s\"\"\")"""

    println(sa)
    val dfj = ddf1.join(ddf2,ex,"LeftOuter")
    dfj.show()

    val ex1 = org.apache.spark.sql.functions.expr("(((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((`GMBS_B2_TRAN_CD` <=> `GMBS_B2_TRAN_CD`) AND (`GMBS_MRCH_REGN_CD` <=> `GMBS_MRCH_REGN_CD`)) AND (`GMBS_BIN_TYP` <=> `GMBS_BIN_TYP`)) AND (`GMBS_SETLMT_SVC_ID` <=> `GMBS_SETLMT_SVC_ID`)) AND (`GMBS_REIMB_ATTR` <=> `GMBS_REIMB_ATTR`)) AND (`GMBS_POS_CHK_OCCURS` <=> `GMBS_POS_CHK_OCCURS`)) AND (`GMBS_PPD_CRD_IND` <=> `GMBS_PPD_CRD_IND`)) AND (`GMBS_RTN_FLG` <=> `GMBS_RTN_FLG`)) AND (`GMBS_EXPRT_FLG` <=> `GMBS_EXPRT_FLG`)) AND (`GMBS_COLL_ONLY_FLG` <=> `GMBS_COLL_ONLY_FLG`)) AND (`GMBS_JP_PYMT_MODE` <=> `GMBS_JP_PYMT_MODE`)) AND (`GMBS_USAGE_CD` <=> `GMBS_USAGE_CD`)) AND (`GMBS_CPS_FLG` <=> `GMBS_CPS_FLG`)) AND (`GMBS_JP_INSTL_PYMT_CNT` <=> `GMBS_JP_INSTL_PYMT_CNT`)) AND (`GMBS_SSBC_CURR_CD` <=> `GMBS_SSBC_CURR_CD`)) AND (`GMBS_TRAN_CD_QLFR` <=> `GMBS_TRAN_CD_QLFR`)) AND (`GMBS_CURR_CONV_IND` <=> `GMBS_CURR_CONV_IND`)) AND (`GMBS_PROD_TYP_CD` <=> `GMBS_PROD_TYP_CD`)) AND (`GMBS_SML_TKT_EXCPTN_IND` <=> `GMBS_SML_TKT_EXCPTN_IND`)) AND (`GMBS_ELMT_RPTG_JRSDCTN_ID` <=> `GMBS_ELMT_RPTG_JRSDCTN_ID`)) AND (`GMBS_ELMT_CUSTM_PROD_GRP_ID` <=> `GMBS_ELMT_CUSTM_PROD_GRP_ID`)) AND (`GMBS_ELMT_NTWRK_ID` <=> `GMBS_ELMT_NTWRK_ID`)) AND (`GMBS_ELMT_CRDH_AUTH_MTHD_ID` <=> `GMBS_ELMT_CRDH_AUTH_MTHD_ID`)) AND (`GMBS_ELMT_ACPTNC_CHNL_ID` <=> `GMBS_ELMT_ACPTNC_CHNL_ID`)) AND (`GMBS_ELMT_MRCH_TYP_GRP_ID` <=> `GMBS_ELMT_MRCH_TYP_GRP_ID`)) AND (`GMBS_ELMT_TKT_SIZE_ID` <=> `GMBS_ELMT_TKT_SIZE_ID`)) AND (`GMBS_PROD_ID_NUM` <=> `GMBS_PROD_ID_NUM`)) AND (`GMBS_PROC_TRAN_CD` <=> `GMBS_PROC_TRAN_CD`)) AND (`GMBS_PPD_SUB_TYP_CD` <=> `GMBS_PPD_SUB_TYP_CD`)) AND (`GMBS_POS_ENV_CD` <=> `GMBS_POS_ENV_CD`)) AND (`GMBS_POSENTRY_MODE_CD` <=> `GMBS_POSENTRY_MODE_CD`)) AND (`GMBS_CRD_ID_MTH_CD` <=> `GMBS_CRD_ID_MTH_CD`)) AND (`GMBS_SETLMT_CD` <=> `GMBS_SETLMT_CD`)) AND (`GMBS_ELMT_TERM_AUTH_CPBTY_ID` <=> `GMBS_ELMT_TERM_AUTH_CPBTY_ID`)) AND (`GMBS_PAISLEY_QUALIFIED` <=> `GMBS_PAISLEY_QUALIFIED`)) AND (`GMBS_MVV_DBA_COMPLNC_IND_DRVD` <=> `GMBS_MVV_DBA_COMPLNC_IND_DRVD`)) AND (`GMBS_SPCL_PRC_ELMT_MRCH_TYP_GRP_NO_MVV_ID_DRVD` <=> `GMBS_SPCL_PRC_ELMT_MRCH_TYP_GRP_NO_MVV_ID_DRVD`)) AND (`GMBS_AISA_ELIG_CD` <=> `GMBS_AISA_ELIG_CD`)) AND (`GMBS_IAF_ELIG_CD` <=> `GMBS_IAF_ELIG_CD`)) AND (`GMBS_ISA_ELIG_CD` <=> `GMBS_ISA_ELIG_CD`)) AND (`GMBS_ACQR_INITD_SNGL_CURR_IND` <=> `GMBS_ACQR_INITD_SNGL_CURR_IND`)) AND (`GMBS_TOKN_TRAN_IND` <=> `GMBS_TOKN_TRAN_IND`)) AND (`GMBS_TOKN_TYP_CD` <=> `GMBS_TOKN_TYP_CD`)) AND (`GMBS_BUS_APPL_ID` <=> `GMBS_BUS_APPL_ID`)) AND (`GMBS_MMAP_QLFD_TRAN_IND` <=> `GMBS_MMAP_QLFD_TRAN_IND`)) AND (`CMLS_TRAN_SEQ_ID` <=> `CMLS_TRAN_SEQ_ID`)) AND (`GMBS_IAF_CHRG_IND_TYP` <=> `GMBS_IAF_CHRG_IND_TYP`)) AND (`GMBS_IAF_SRCHG_IND_TYP` <=> `GMBS_IAF_SRCHG_IND_TYP`)) AND (`GMBS_B2B_ACQR_FEE_ELG_IND` <=> `GMBS_B2B_ACQR_FEE_ELG_IND`)) AND (`GMBS_TRAN_PROCG_FEE_IND` <=> `GMBS_TRAN_PROCG_FEE_IND`)) AND (`GMBS_ACQR_NTWRK_ID_CRISCROS` <=> `GMBS_ACQR_NTWRK_ID_CRISCROS`)) AND (`GMBS_ISSR_NTWRK_ID_CRISCROS` <=> `GMBS_ISSR_NTWRK_ID_CRISCROS`)) AND (`GMBS_DCC_IND` <=> `GMBS_DCC_IND`)) AND (`GMBS_FLXBL_CMSN_RT_QLFD_IND` <=> `GMBS_FLXBL_CMSN_RT_QLFD_IND`)) AND (`GMBS_SRCHG_AMT` <=> `GMBS_SRCHG_AMT`)) AND (`GMBS_BNK_P2P_NEGT_RT_IND` <=> `GMBS_BNK_P2P_NEGT_RT_IND`)) AND (`gmbs_cut_num_drvd` <=> `gmbs_cut_num_drvd`)) AND (`GMBS_RPTG_JRSDCTN` <=> `GMBS_RPTG_JRSDCTN`)) AND (`GMBS_SMS_MSG_CLS` <=> `GMBS_SMS_MSG_CLS`)) AND (`GMBS_PROC_CD` <=> `GMBS_PROC_CD`)) AND (`GMBS_RESP_CD` <=> `GMBS_RESP_CD`)) AND (`GMBS_SMS_MIS_CAS_RSN_CD` <=> `GMBS_SMS_MIS_CAS_RSN_CD`)) AND (`GMBS_POSENTRY_CAP` <=> `GMBS_POSENTRY_CAP`)) AND (`GMBS_SMS_NTWRK_MGMT_CD` <=> `GMBS_SMS_NTWRK_MGMT_CD`)) AND (`GMBS_SMS_PREAUTH_TIME_LMT` <=> `GMBS_SMS_PREAUTH_TIME_LMT`)) AND (`GMBS_SMS_FILE_UPDT_CD` <=> `GMBS_SMS_FILE_UPDT_CD`)) AND (`GMBS_POSENTRY_MODE_DATA` <=> `GMBS_POSENTRY_MODE_DATA`)) AND (`GMBS_ACCT_TYP_TO` <=> `GMBS_ACCT_TYP_TO`)) AND (`GMBS_SMS_STOP_ORD_TYP` <=> `GMBS_SMS_STOP_ORD_TYP`)) AND (`GMBS_SMS_STIP_SW_RSN_CD` <=> `GMBS_SMS_STIP_SW_RSN_CD`)) AND (`GMBS_DG_PRTCPTN_FLG` <=> `GMBS_DG_PRTCPTN_FLG`)) AND (`GMBS_DG_TRAN_XCPT_FLG` <=> `GMBS_DG_TRAN_XCPT_FLG`)) AND (`GMBS_VISA_PROC_CHRG_ACQR_SRE_ID` <=> `GMBS_VISA_PROC_CHRG_ACQR_SRE_ID`)) AND (`GMBS_ACQR_STN_FLG` <=> `GMBS_ACQR_STN_FLG`)) AND (`GMBS_CRD_ID_MTHD_CD` <=> `GMBS_CRD_ID_MTHD_CD`)) AND (`GMBS_FM_UPDATING_PCR` <=> `GMBS_FM_UPDATING_PCR`)) AND (`GMBS_CRISS_CROS_IND` <=> `GMBS_CRISS_CROS_IND`)) AND (`GMBS_MSG_FMT_DNGRD_IND` <=> `GMBS_MSG_FMT_DNGRD_IND`)) AND (`GMBS_MSG_FMT_UPGRD_IND` <=> `GMBS_MSG_FMT_UPGRD_IND`)) AND (`GMBS_CAM_PERF_IND` <=> `GMBS_CAM_PERF_IND`)) AND (`GMBS_CAM_RSLT_NOT_ACTD_UPON_IND` <=> `GMBS_CAM_RSLT_NOT_ACTD_UPON_IND`)) AND (`GMBS_DCVV_PERF_IND` <=> `GMBS_DCVV_PERF_IND`)) AND (`GMBS_ICVV_CONVRSN_PERFD_IND` <=> `GMBS_ICVV_CONVRSN_PERFD_IND`)) AND (`GMBS_VMP_FLG` <=> `GMBS_VMP_FLG`)) AND (`GMBS_AFFS_ACQR_PRTCPTN_IND` <=> `GMBS_AFFS_ACQR_PRTCPTN_IND`)) AND (`GMBS_AFFS_ISSR_PRTCPTN_IND` <=> `GMBS_AFFS_ISSR_PRTCPTN_IND`)) AND (`GMBS_AFFS_TRAN_QUAL_IND` <=> `GMBS_AFFS_TRAN_QUAL_IND`)) AND (`GMBS_VTA_EC_MRCH_RISK_SNT_TO_ACQR_IND` <=> `GMBS_VTA_EC_MRCH_RISK_SNT_TO_ACQR_IND`)) AND (`GMBS_ATM_TIER_LVL_DRVD` <=> `GMBS_ATM_TIER_LVL_DRVD`)) AND (`GMBS_OCT_TO_MRT_DNGRD_FLG` <=> `GMBS_OCT_TO_MRT_DNGRD_FLG`)) AND (`GMBS_VAU_SVC_ID` <=> `GMBS_VAU_SVC_ID`)) AND (`GMBS_RT_VAU_STA_CD` <=> `GMBS_RT_VAU_STA_CD`)) AND (`GMBS_VAU_PPCS_UPD_IND` <=> `GMBS_VAU_PPCS_UPD_IND`)) AND (`GMBS_TSP_TOKN_IND` <=> `GMBS_TSP_TOKN_IND`)) AND (`GMBS_VE_MBR_INTGRTN_STA` <=> `GMBS_VE_MBR_INTGRTN_STA`)) AND (`GMBS_INCRCT_ATM_RTEG_TBL_UNIQ_ID` <=> `GMBS_INCRCT_ATM_RTEG_TBL_UNIQ_ID`)) AND (`GMBS_CVV2_CONVRSN_PERFD_IND` <=> `GMBS_CVV2_CONVRSN_PERFD_IND`)) AND (`GMBS_B1ADVC_CCPS_TRAN_IND` <=> `GMBS_B1ADVC_CCPS_TRAN_IND`)) AND (`GMBS_SVC_CD` <=> `GMBS_SVC_CD`)) AND (`GMBS_DCVV2_GENRTN_CNT` <=> `GMBS_DCVV2_GENRTN_CNT`)) AND (`GMBS_SMS_CRD_AUTH_RESP_CD` <=> `GMBS_SMS_CRD_AUTH_RESP_CD`)) AND (`GMBS_TB_DCVV2_VLDTN_PERFD_IND` <=> `GMBS_TB_DCVV2_VLDTN_PERFD_IND`)) AND (`GMBS_BUS_APPLN_ID` <=> `GMBS_BUS_APPLN_ID`)) AND (smssmsincomingtable.`GMBS_TOKN_REQSTR_BUS_ID` <=> smssmsincomingtable.`GMBS_TOKN_REQSTR_BUS_ID`)) AND (smssmsincomingtable.`GMBS_TOKN_OBO_BILL_IND` <=> smssmsincomingtable.`GMBS_TOKN_OBO_BILL_IND`)) AND (smssmsincomingtable.`GMBS_ON_US_IND` <=> smssmsincomingtable.`GMBS_ON_US_IND`)) AND (smssmsincomingtable.`GMBS_REBATE_BID` <=> smssmsincomingtable.`GMBS_REBATE_BID`)) AND (smssmsincomingtable.`GMBS_ACQR_PCR_FLG` <=> smssmsincomingtable.`GMBS_ACQR_PCR_FLG`)) AND (smssmsincomingtable.`GMBS_ATM_SHARE_FLG` <=> smssmsincomingtable.`GMBS_ATM_SHARE_FLG`)) AND (smssmsincomingtable.`GMBS_XBONUS_IND_DRVD` <=> smssmsincomingtable.`GMBS_XBONUS_IND_DRVD`)) AND (`GMBS_SMS_REC_TYP` <=> `GMBS_SMS_REC_TYP`)) AND (`GMBS_REC_SEQ_NUM` <=> `GMBS_REC_SEQ_NUM`)) AND (`GMBS_REC_TYP` <=> `GMBS_REC_TYP`)) AND (`GMBS_SETLMT_FILE_TYP` <=> `GMBS_SETLMT_FILE_TYP`)) AND (`GMBS_POS_RESP_SRCE_RSN_CD` <=> `GMBS_POS_RESP_SRCE_RSN_CD`)) AND (`GMBS_SMS_DIR_DB_SW_IND` <=> `GMBS_SMS_DIR_DB_SW_IND`)) AND (`GMBS_COLL_FLAG` <=> `GMBS_COLL_FLAG`)) AND (`GMBS_IN_OUT_FLG` <=> `GMBS_IN_OUT_FLG`)) AND (`GMBS_POS_CHK_SETLMT_CD` <=> `GMBS_POS_CHK_SETLMT_CD`)) AND (`GMBS_TRAN_CNT` <=> `GMBS_TRAN_CNT`)) AND (smssmsincomingtable.`gmbs_billing_cylce_id` <=> smssmsincomingtable.`gmbs_billing_cylce_id`)) AND (`GMBS_TSP_BID` <=> `GMBS_TSP_BID`)) AND (`GMBS_BID` <=> `GMBS_BID`)) AND (`GMBS_IOI_SA_SRCE_FLG` <=> `GMBS_IOI_SA_SRCE_FLG`)) AND (`GMBS_FEECOLL_RSN_CD` <=> `GMBS_FEECOLL_RSN_CD`)) AND (`GMBS_MULTINATL_ACQR_PGM_CTRY_CD_DRVD` <=> `GMBS_MULTINATL_ACQR_PGM_CTRY_CD_DRVD`)) AND (`GMBS_MRCH_CTRY_CD` <=> `GMBS_MRCH_CTRY_CD`)) AND (`GMBS_ECI_MOTO_CD` <=> `GMBS_ECI_MOTO_CD`)) AND (`GMBS_SMS_TGT_FILE_NM_CD` <=> `GMBS_SMS_TGT_FILE_NM_CD`)) AND (`GMBS_ADDL_TRC_DATA` <=> `GMBS_ADDL_TRC_DATA`)) AND (`GMBS_PCR` <=> `GMBS_PCR`)) AND (`GMBS_OTH_BIN_REGN_CD` <=> `GMBS_OTH_BIN_REGN_CD`)) AND (`GMBS_NTWRK_ID` <=> `GMBS_NTWRK_ID`)) AND (`GMBS_TRAN_COLL_FROM_FLG` <=> `GMBS_TRAN_COLL_FROM_FLG`)) AND (`GMBS_DEST_FLG` <=> `GMBS_DEST_FLG`)) AND (`GMBS_SRE_ID` <=> `GMBS_SRE_ID`)) AND (`GMBS_XCPT_CHRG_IND` <=> `GMBS_XCPT_CHRG_IND`)) AND (`GMBS_IOI_SA_SRCE_AMT` <=> `GMBS_IOI_SA_SRCE_AMT`)) AND (`GMBS_B2_CHRG_AMT` <=> `GMBS_B2_CHRG_AMT`)) AND (`GMBS_BIN_SETLMT_ID` <=> `GMBS_BIN_SETLMT_ID`)) AND (`GMBS_MULT_CURR_FLG` <=> `GMBS_MULT_CURR_FLG`)) AND (`GMBS_CDF_IND` <=> `GMBS_CDF_IND`)) AND (`GMBS_SMS_DEST_APPL_PROC` <=> `GMBS_SMS_DEST_APPL_PROC`)) AND (`GMBS_TIF_IND` <=> `GMBS_TIF_IND`)) AND (`GMBS_TOKN_AVS_IND` <=> `GMBS_TOKN_AVS_IND`)) AND (`GMBS_CCC_ISC_AMT` <=> `GMBS_CCC_ISC_AMT`)) AND (`GMBS_RTRN_RSN_CD` <=> `GMBS_RTRN_RSN_CD`)) AND (`GMBS_RTRN_RSN_CD_1` <=> `GMBS_RTRN_RSN_CD_1`)) AND (`GMBS_RTRN_RSN_FLG` <=> `GMBS_RTRN_RSN_FLG`)) AND (`GMBS_SPCL_ATM_FEE_IND` <=> `GMBS_SPCL_ATM_FEE_IND`)) AND (`GMBS_FEE_AMT` <=> `GMBS_FEE_AMT`)) AND (`GMBS_DG_ACQR_CHRG_SC_AMT` <=> `GMBS_DG_ACQR_CHRG_SC_AMT`)) AND (`GMBS_AISA_SC_AMT` <=> `GMBS_AISA_SC_AMT`)) AND (`GMBS_AISA_IND` <=> `GMBS_AISA_IND`)) AND (`GMBS_ISA_IND` <=> `GMBS_ISA_IND`)) AND (`GMBS_ECOM_IAF_SC_AMT` <=> `GMBS_ECOM_IAF_SC_AMT`)) AND (`GMBS_MULT_CURR_SETLMT_CHRG_AMT_SC` <=> `GMBS_MULT_CURR_SETLMT_CHRG_AMT_SC`)) AND (`GMBS_ACCT_PROD_ID` <=> `GMBS_ACCT_PROD_ID`)) AND (`GMBS_POS_VIP_PROC_FLG` <=> `GMBS_POS_VIP_PROC_FLG`)) AND (`GMBS_POS_ACQR_ODFI_FLG` <=> `GMBS_POS_ACQR_ODFI_FLG`)) AND (`GMBS_POS_CONV_TRAN_FLG` <=> `GMBS_POS_CONV_TRAN_FLG`)) AND (`GMBS_POS_VRFCN_TRAN_FLG` <=> `GMBS_POS_VRFCN_TRAN_FLG`)) AND (`GMBS_POS_GUAR_TRAN_FLG` <=> `GMBS_POS_GUAR_TRAN_FLG`)) AND (`GMBS_POS_ACH_PROC_FLG` <=> `GMBS_POS_ACH_PROC_FLG`)) AND (`GMBS_POSCOND_CD` <=> `GMBS_POSCOND_CD`)) AND (`GMBS_CCC_ALLOC_ACQR_AMT` <=> `GMBS_CCC_ALLOC_ACQR_AMT`)) AND (`GMBS_CCC_ALLOC_ACQR_REG_AMT` <=> `GMBS_CCC_ALLOC_ACQR_REG_AMT`)) AND (`GMBS_CCC_ALLOC_ISSR_AMT` <=> `GMBS_CCC_ALLOC_ISSR_AMT`)) AND (`GMBS_CCC_ALLOC_ISSR_REG_AMT` <=> `GMBS_CCC_ALLOC_ISSR_REG_AMT`)) AND (`GMBS_GIV_UPD_FLG` <=> `GMBS_GIV_UPD_FLG`)) AND (`GMBS_BIN` <=> `GMBS_BIN`)) AND (`GMBS_OTH_BIN_CTRY_CD` <=> `GMBS_OTH_BIN_CTRY_CD`)) AND (`GMBS_AUTH_ONLY_IND` <=> `GMBS_AUTH_ONLY_IND`)) AND (`GMBS_ELMT_DURBIN_EXMPT_IND` <=> `GMBS_ELMT_DURBIN_EXMPT_IND`)) AND (`GMBS_BIN_NEW` <=> `GMBS_BIN_NEW`)) AND (`GMBS_CRD_TYP` <=> `GMBS_CRD_TYP`)) AND (`GMBS_CSHBK_AMT_USD` <=> `GMBS_CSHBK_AMT_USD`)) AND (`GMBS_RESP_SRCE` <=> `GMBS_RESP_SRCE`)) AND (`GMBS_PROD_BRND_CD` <=> `GMBS_PROD_BRND_CD`)) AND (`GMBS_PCR_REGN_CD` <=> `GMBS_PCR_REGN_CD`)) AND (`GMBS_PCR_CTRY_CD` <=> `GMBS_PCR_CTRY_CD`)) AND (`GMBS_KYC_MIXED_IND_VCIS` <=> `GMBS_KYC_MIXED_IND_VCIS`)) AND (`GMBS_ACCT_LVL_PROCG_CD_VCIS` <=> `GMBS_ACCT_LVL_PROCG_CD_VCIS`)) AND (`GMBS_PROD_ID_SUBTYP_CD` <=> `GMBS_PROD_ID_SUBTYP_CD`)) AND (`GMBS_ACCT_FUNDG_SUBTYP_CD` <=> `GMBS_ACCT_FUNDG_SUBTYP_CD`)) AND (`GMBS_ACCT_FUNDG_SRCE_CD` <=> `GMBS_ACCT_FUNDG_SRCE_CD`)) AND (`GMBS_BIN_CTRY_CD` <=> `GMBS_BIN_CTRY_CD`)) AND (`GMBS_CDF_AMT` <=> `GMBS_CDF_AMT`)) AND (`GMBS_TIF_SC_AMT` <=> `GMBS_TIF_SC_AMT`)) AND (`GMBS_PROD_ID_PLTFRM_CD` <=> `GMBS_PROD_ID_PLTFRM_CD`)) AND (`GMBS_PROD_ID_CLS_CD` <=> `GMBS_PROD_ID_CLS_CD`)) AND (`GMBS_PROD_ID_SCHM_CD` <=> `GMBS_PROD_ID_SCHM_CD`)) AND (`GMBS_TRAN_AMT_USD` <=> `GMBS_TRAN_AMT_USD`)) AND (`GMBS_BIN_REGN_CD` <=> `GMBS_BIN_REGN_CD`)) AND (`gmbs_orig_msg_typ` <=> `gmbs_orig_msg_typ`)) AND (`GMBS_VDPS_ACQR_BIN_MIGR_ID` <=> `GMBS_VDPS_ACQR_BIN_MIGR_ID`)) AND (`GMBS_VDPS_ISSR_BIN_MIGR_ID` <=> `GMBS_VDPS_ISSR_BIN_MIGR_ID`)) AND (`GMBS_INTRCHG_FEE_ACQR_SRE_ID` <=> `GMBS_INTRCHG_FEE_ACQR_SRE_ID`)) AND (`GMBS_PARTIAL_AUTH_IND` <=> `GMBS_PARTIAL_AUTH_IND`)) AND (`GMBS_PIN_TRN_FLG` <=> `GMBS_PIN_TRN_FLG`)) AND (`GMBS_PIN_VRFCN_FLG` <=> `GMBS_PIN_VRFCN_FLG`)) AND (`GMBS_INTNL_SVC_ID` <=> `GMBS_INTNL_SVC_ID`)) AND (`GMBS_SPECIAL_ROUTING_IND` <=> `GMBS_SPECIAL_ROUTING_IND`)) AND (`GMBS_PCAS_ACTV_TSTD_IND` <=> `GMBS_PCAS_ACTV_TSTD_IND`)) AND (`GMBS_MRCH_VRFCN_VAL` <=> `GMBS_MRCH_VRFCN_VAL`)) AND (`GMBS_TRAN_DRCTN` <=> `GMBS_TRAN_DRCTN`)) AND (`gmbs_dg_acqr_chrg_sre_id` <=> `gmbs_dg_acqr_chrg_sre_id`)) AND (`GMBS_MSG_TYP` <=> `GMBS_MSG_TYP`)) AND (`GMBS_ACCT_USAGE_CD` <=> `GMBS_ACCT_USAGE_CD`)) AND (smssmsincomingtable.`CMLS_RECORD_INDEX_DRVD` <=> smssmsincomingtable.`CMLS_RECORD_INDEX_DRVD`)) AND (`GMBS_CIB` <=> `GMBS_CIB`)) AND (`GMBS_MRCH_DBA_ID` <=> `GMBS_MRCH_DBA_ID`)) AND (`GMBS_SMS_OTH_BIN` <=> `GMBS_SMS_OTH_BIN`)) AND (smssmsincomingtable.`gmbs_cpd_dt` <=> smssmsincomingtable.`gmbs_cpd_dt`)) AND (`gmbs_mrch_catg_cd` <=> `gmbs_mrch_catg_cd`)) AND (`gmbs_setlmt_curr_cd` <=> `gmbs_setlmt_curr_cd`)) AND (smssmsincomingtable.`CMLS_ORIGTR_SUBMTD_REQST_ADVC_TRAN_AMT_FLG` <=> smssmsincomingtable.`CMLS_ORIGTR_SUBMTD_REQST_ADVC_TRAN_AMT_FLG`)) AND (smssmsincomingtable.`cmls_uniq_seq_id_drvd` <=> smssmsincomingtable.`cmls_uniq_seq_id_drvd`)) AND (`GMBS_TXN_DATA_SOURCE` <=> `GMBS_TXN_DATA_SOURCE`)) AND (`cpd_dt` <=> `cpd_dt`)) AND (`gmbs_data_source` <=> `gmbs_data_source`)) AND (opcode.tedc_procg_id.`prmy_procg_id` <=> opcode.tedc_procg_id.`prmy_procg_id`)) AND (opcode.tedc_procg_id.`regn_cd_drvd` <=> opcode.tedc_procg_id.`regn_cd_drvd`)) AND (opcode.tedc_procg_id.`ctry_cd` <=> opcode.tedc_procg_id.`ctry_cd`)) AND (`CMLS_VE_REJ_FLG` <=> `CMLS_VE_REJ_FLG`))")

    val ex1String = scalaString(ex1.expr,false)

    val hs  = "Hello,Hi".split(",")
    sc.parallelize(hs)
    val v1 = "hello"
    val df = sqlContext.sql(
      s"""Select a.k1,a.v1, b.v2, b2drfRejFlgUDF("hello","hi",1,"bye") as gm1,b2otcRejFlgUDF("hello","hi","bye") as gm2,cmlsJrsdctnUDF("hello","hi","bye",a.k1,a.v1,"bbye","Hii",b.v2) as gm3,
         gmbsBinCtryCdIncomingUDF("a","b","c","d","e","f") as gm4,gmbsBinCtryCdIncomingOthUDF("a","b","c","d","e","f") as gm5,gmbsBinCtryCdOutgoingUDF("a","b","c","d","e","f") as gm6,
         gmbsBinCtryCdOutgoingOthUDF("a","b","c","d","e","f") as gm7,gmbsBinRegnCdIncomingUDF("a","b","c","d","e") as gm8,
         gmbsBinRegnCdIncomingOthUDF("a","b","c","d","e") as gm9,gmbsBinRegnCdOutgoingUDF("a","b","c","d","e") as gm10 ,
         gmbsBinRegnCdOutgoingOthUDF("a","v","c","d","e") as gm11,gmbsBinOrigCtryCdIncomingUDF("a","b","c") as gm12,gmbsBinOrigCtryCdOutgoingUDF("a","b","c") as gm13
         from df1 as a LEFT OUTER JOIN df2 as b on a.k1 = b.k2""")

    df.queryExecution.logical
    val mp = Map("gblcs.tcmls_edwcs_b2smsdrf_dtl_avro.`CMLS_SRCE_CIB`"->"`CMLS_SRCE_CIB`",
                 "gblcs.tcmls_edwcs_b2smsdrf_dtl_avro.`CMLS_NSPK_FALLBK_PROCG_RSN_CD`"->"`CMLS_NSPK_FALLBK_PROCG_RSN_CD`",
                "UDF:"->""
                )
    val selects:String = "CASE WHEN ((IF(((gblcs.tcmls_edwcs_b2smsdrf_dtl_avro.`CMLS_SRCE_CIB` IS NULL) OR (gblcs.tcmls_edwcs_b2smsdrf_dtl_avro.`CMLS_NSPK_FALLBK_PROCG_RSN_CD` IS NULL)), CAST(NULL AS INT), UDF:rejFlagSmsDrfUDF(CMLS_DEST_FLG, CMLS_VE_CS_JRSDCTN_CD_ENR, CMLS_SRCE_CIB, CMLS_NSPK_FALLBK_PROCG_RSN_CD, CMLS_VIP_RSI_MSG_CATGY, CMLS_TRAN_DRCTN, CMLS_SMS_REQ_MSG_TYP, CMLS_REC_TYP))) = 0) THEN 'GS' ELSE 'GR' END"
    //val newselects = selects.replaceAll("gblcs.tcmls_edwcs_b2smsdrf_dtl_avro.`CMLS_SRCE_CIB`","`CMLS_SRCE_CIB`")

    val newS = removeQualifiers(selects,mp)
    println(newS)
    println("Before serialization")
    //    df.take(2).foreach(println)
    //test for spark catalyst sql member value

    val ex_reformat = expr("CASE WHEN ((IF(((df1.v1 IS NULL) OR (df1.k1 IS NULL)), CAST(NULL AS INT), df1.k1)) = 0) THEN 'GS' ELSE 'GR' END AS `gmbs_process_code`")

   val df2 =  df.select("a.k1").where("((a.k1 IS NOT NULL) AND (NOT (a.k1 = 0)))")
    import org.apache.spark.sql.expressions._
   //val df3 = df2.withColumn("nc",power3(2).toString)
    val df3 = df2.selectExpr(s"""b2drfRejFlgUDF("hello","hi",1,"bye") as colN""")
    val df4 = df2.withColumn("tt",org.apache.spark.sql.functions.expr(s"""b2drfRejFlgUDF("hello","hi",1,"bye")"""))

    print(df2.queryExecution.analyzed.prettyJson)
    val data = Array(df.queryExecution.analyzed)
    val rdd = sc.parallelize(data).map[LogicalPlanRDD](x => LogicalPlanRDD(x))

  //  KryoSerializerDeserializer().saveAsObjectFile[LogicalPlanRDD](rdd, "hdfs:///tmp/maciej/b2DRFOutputLogicalPlan")

//    val deserializedRDD = KryoSerializerDeserializer().objectFile[LogicalPlanRDD](sc, "./rxx")
//    val logicalPlan = deserializedRDD.collect()(0).logicalPlan
//    print(logicalPlan.prettyJson)

    //val newDf = DataFrame(sqlContext, logicalPlan)
    println("After serialization")
   // df.take(2).foreach(println)
  }

  def power3(value:Double,v1:String):String={
    value*value*value+v1
  }

  def scalaString(expr: Expression,isleft: Boolean): String = {

    if(expr.isInstanceOf[And]){
      val a = expr.asInstanceOf[And]
      s"${scalaString(a.left,true)}&&${scalaString(a.right,false)}"
    }else if(expr.isInstanceOf[EqualTo]){
      val e = expr.asInstanceOf[EqualTo]
      s"${scalaString(e.left,true)} === ${scalaString(e.right,false)}"
    }else if(expr.isInstanceOf[EqualNullSafe]){
      val e = expr.asInstanceOf[EqualNullSafe]
      s"${scalaString(e.left,true)} <=> ${scalaString(e.right,false)}"
    }else if(expr.isInstanceOf[AttributeReference]){
      val a = expr.asInstanceOf[AttributeReference]
      if(isleft)
        s"left(${a.name})"
      else
        s"right(${a.name})"

    }else{
      val a = expr.asInstanceOf[UnresolvedAttribute]
      if(isleft)
        s"left(${a.name})"
      else
        s"right(${a.name})"
    }
  }

  def quoteIdentifier(name: String): String = {
    // Escapes back-ticks within the identifier name with double-back-ticks, and then quote the
    // identifier with back-ticks.
    "`" + name.replace("`", "``") + "`"
  }

  def removeQualifiers(s:String,mp:Map[String,String]):String = {
    var result = s
    mp.map(i=>{
      result = result.replaceAll(i._1,i._2)
    })
    result
  }


}

class KryoSerializerDeserializer {
  def saveAsObjectFile[A : ClassTag](rdd : RDD[A], path : String): Unit = {
    val kryoSerializer = new KryoSerializer(rdd.context.getConf)

    rdd.mapPartitions(iter => iter.grouped(10)
      .map(_.toArray))
      .map(splitArray => {
        val kryo = kryoSerializer.newKryo()

        val bao = new ByteArrayOutputStream()
        val output = kryoSerializer.newKryoOutput()
        output.setOutputStream(bao)
        kryo.writeClassAndObject(output, splitArray)
        output.close()

        val byteWritable = new BytesWritable(bao.toByteArray)
        (NullWritable.get(), byteWritable)
      }).saveAsSequenceFile(path)
  }

  def objectFile[A : ClassTag](sc : SparkContext, path : String, minPartitions: Int = 1)(implicit tt : TypeTag[A]) = {
    val kryoSerializer = new KryoSerializer(sc.getConf)

    sc.sequenceFile(path, classOf[NullWritable], classOf[BytesWritable], minPartitions)
      .flatMap(x => {
        val kryo = kryoSerializer.newKryo()
        val input = new Input()
        input.setBuffer(x._2.getBytes)
        val data = kryo.readClassAndObject(input)
        val dataObject = data.asInstanceOf[Array[A]]
        dataObject
      })
  }




}



object KryoSerializerDeserializer {
  def apply() = new KryoSerializerDeserializer
}
