import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.Column
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

trait B2CommonUDFs {

  def cleanse(x: Column): Column = {
    regexp_replace(x, "[^\\x20-\\x7F]+", " ")
  }

  val b2drfRejFlg = udf((CMLS_DEST_FLG: String, CMLS_VE_CS_JRSDCTN_CD_ENR: String, CMLS_SRCE_CIB: Integer, cmls_vip_rsi_msg_catgy: String) =>
  {
    if (CMLS_DEST_FLG == "V" || ((CMLS_VE_CS_JRSDCTN_CD_ENR == null || CMLS_VE_CS_JRSDCTN_CD_ENR.trim() == "") && CMLS_SRCE_CIB == 400431)) {
      "2"
    } else if ((cmls_vip_rsi_msg_catgy.reverse.padTo(5,'0').reverse != "00148") && (cmls_vip_rsi_msg_catgy.reverse.padTo(5,'0').reverse != "00149")) {
      if (CMLS_DEST_FLG != "E") {
        "0"
      } else {
        "1"
      }
    } else {
      "2"
    }
  })


  val b2OtcRejFlg = udf((CMLS_DEST_FLG: String, CMLS_VCR_REC_ID:String, CMLS_TRAN_CD:String) =>
  {

    if(CMLS_VCR_REC_ID.equalsIgnoreCase("VCR"))
    {
      "2"
    }
    else if(CMLS_TRAN_CD.equals("32") || CMLS_TRAN_CD.equals("44") || CMLS_TRAN_CD.equals("54"))
    {
      "1"
    }
    else if (CMLS_DEST_FLG != "E") {
      "0"
    } else {
      "1"
    }
  }
  )

  val cmlsJrsdctnUDF = udf((CMLS_TRAN_CD: String, CMLS_SRCE_BIN_REGN_CD: String, CMLS_DEST_BIN_REGN_CD: String, CMLS_SRCE_BIN_ZONE_CD: Int, CMLS_DEST_BIN_ZONE_CD: Int, CMLS_RPTG_JRSDCTN: String, CMLS_MRCH_REGN_CD: String, CMLS_MRCH_ZONE_CD: Int) => {

    var mrch_reg = CMLS_MRCH_REGN_CD
    var mrch_zone = CMLS_MRCH_ZONE_CD
    var rptg_jrsdctn = CMLS_RPTG_JRSDCTN

    var acqr_reg = CMLS_TRAN_CD match {
      case a if (a.equals("05") || a.equals("06") || a.equals("07") || a.equals("25") || a.equals("26") || a.equals("27")) => CMLS_SRCE_BIN_REGN_CD
      case _ => CMLS_DEST_BIN_REGN_CD
    }

    var acqr_zone = CMLS_TRAN_CD match {
      case a if (a.equals("05") || a.equals("06") || a.equals("07") || a.equals("25") || a.equals("26") || a.equals("27")) => CMLS_SRCE_BIN_ZONE_CD
      case _ => CMLS_DEST_BIN_ZONE_CD
    }

    var issr_reg = CMLS_TRAN_CD match {
      case a if (a.equals("05") || a.equals("06") || a.equals("07") || a.equals("25") || a.equals("26") || a.equals("27")) => CMLS_DEST_BIN_ZONE_CD
      case _ => CMLS_SRCE_BIN_REGN_CD
    }

    var issr_zone = CMLS_TRAN_CD match {
      case a if (a.equals("05") || a.equals("06") || a.equals("07") || a.equals("25") || a.equals("26") || a.equals("27")) => CMLS_DEST_BIN_ZONE_CD
      case _ => CMLS_SRCE_BIN_ZONE_CD

    }

    if (mrch_reg.equalsIgnoreCase("00"))
      mrch_reg = acqr_reg
    if (mrch_zone == 0)
      mrch_zone = acqr_zone

    if (!CMLS_RPTG_JRSDCTN.equals("1") && !CMLS_RPTG_JRSDCTN.equals("2") && !CMLS_RPTG_JRSDCTN.equals("3")) {
      if (issr_reg.equals(mrch_reg) && issr_zone.equals(mrch_zone)) {
        "3"
      } else if (issr_reg.equals(mrch_reg)) {
        "2"
      } else {
        "1"
      }
    } else {
      CMLS_RPTG_JRSDCTN
    }

  })

  val gmbsBinCtryCdIncoming = udf((CMLS_SRCE_OF_TRAN_FLG: String, CMLS_ACCT_REGN_CD_DRVD: String, cmls_acct_ctry_cd_drvd: String, cmls_xbrdr_ind: String, CMLS_DEST_BIN_CTRY_CD_DRVD: String, ctry_cd: String) =>
  {
    var gmbs_xbrdr_flg = if (cmls_xbrdr_ind == "Y" && CMLS_SRCE_OF_TRAN_FLG == "A") {
      cmls_xbrdr_ind
    } else {
      "N"
    }

    if (CMLS_SRCE_OF_TRAN_FLG == "I" || cmls_acct_ctry_cd_drvd == " " || cmls_acct_ctry_cd_drvd != "") {
      if (gmbs_xbrdr_flg == "N") {
        CMLS_DEST_BIN_CTRY_CD_DRVD
      } else {
        ctry_cd
      }
    } else if (gmbs_xbrdr_flg == "N") {
      ctry_cd
    } else {
      cmls_acct_ctry_cd_drvd
    }
  })

  val gmbsBinCtryCdIncomingOth = udf((CMLS_SRCE_OF_TRAN_FLG: String, CMLS_ACCT_REGN_CD_DRVD: String, cmls_acct_ctry_cd_drvd: String, cmls_xbrdr_ind: String, CMLS_DEST_BIN_CTRY_CD_DRVD: String, ctry_cd: String) =>
  {
    var gmbs_xbrdr_flg = if (cmls_xbrdr_ind == "Y" && CMLS_SRCE_OF_TRAN_FLG == "A") {
      cmls_xbrdr_ind
    } else {
      "N"
    }

    if (gmbs_xbrdr_flg == "N") {
      CMLS_DEST_BIN_CTRY_CD_DRVD
    } else {
      ctry_cd
    }
  })

  val gmbsBinCtryCdOutgoing = udf((CMLS_SRCE_OF_TRAN_FLG: String, CMLS_ACCT_REGN_CD_DRVD: String, cmls_acct_ctry_cd_drvd: String, cmls_xbrdr_ind: String, CMLS_SRCE_BIN_CTRY_CD_DRVD: String, ctry_cd: String) =>
  {
    var gmbs_xbrdr_flg = if (cmls_xbrdr_ind == "Y" && CMLS_SRCE_OF_TRAN_FLG == "A") {
      cmls_xbrdr_ind
    } else {
      "N"
    }

    if (CMLS_SRCE_OF_TRAN_FLG == "A" || cmls_acct_ctry_cd_drvd == " " || cmls_acct_ctry_cd_drvd != "") {
      if (gmbs_xbrdr_flg == "N") {
        CMLS_SRCE_BIN_CTRY_CD_DRVD
      } else {
        ctry_cd
      }
    } else if (gmbs_xbrdr_flg == "N") {
      CMLS_SRCE_BIN_CTRY_CD_DRVD
    } else {
      cmls_acct_ctry_cd_drvd
    }
  })

  val gmbsBinCtryCdOutgoingOth = udf((CMLS_SRCE_OF_TRAN_FLG: String, CMLS_ACCT_REGN_CD_DRVD: String, cmls_acct_ctry_cd_drvd: String, cmls_xbrdr_ind: String, CMLS_SRCE_BIN_CTRY_CD_DRVD: String, ctry_cd: String) =>
  {
    var gmbs_xbrdr_flg = if (cmls_xbrdr_ind == "Y" && CMLS_SRCE_OF_TRAN_FLG == "A") {
      cmls_xbrdr_ind
    } else {
      "N"
    }

    if (gmbs_xbrdr_flg == "N") {
      CMLS_SRCE_BIN_CTRY_CD_DRVD
    } else {
      ctry_cd
    }

  })

  val gmbsBinRegnCdIncoming = udf((CMLS_SRCE_OF_TRAN_FLG: String, CMLS_ACCT_REGN_CD_DRVD: String, cmls_xbrdr_ind: String, CMLS_DEST_BIN_REGN_CD_DRVD: String, CMLS_DEST_BIN_REGN_CD: String) =>
  {

    var gmbs_xbrdr_flg = if (cmls_xbrdr_ind == "Y" && CMLS_SRCE_OF_TRAN_FLG == "A") {
      cmls_xbrdr_ind
    } else {
      "N"
    }

    if (CMLS_SRCE_OF_TRAN_FLG == "I" || CMLS_ACCT_REGN_CD_DRVD == " " || CMLS_ACCT_REGN_CD_DRVD != "" || CMLS_ACCT_REGN_CD_DRVD == "00") {
      if (gmbs_xbrdr_flg == "N") {
        CMLS_DEST_BIN_REGN_CD_DRVD
      } else {
        CMLS_DEST_BIN_REGN_CD
      }
    } else if (gmbs_xbrdr_flg == "N") {
      CMLS_DEST_BIN_REGN_CD_DRVD
    } else {
      CMLS_ACCT_REGN_CD_DRVD
    }
  })

  val gmbsBinRegnCdIncomingOth = udf((CMLS_SRCE_OF_TRAN_FLG: String, CMLS_ACCT_REGN_CD_DRVD: String, cmls_xbrdr_ind: String, CMLS_DEST_BIN_REGN_CD_DRVD: String, CMLS_DEST_BIN_REGN_CD: String) =>
  {

    var gmbs_xbrdr_flg = if (cmls_xbrdr_ind == "Y" && CMLS_SRCE_OF_TRAN_FLG == "A") {
      cmls_xbrdr_ind
    } else {
      "N"
    }

    if (gmbs_xbrdr_flg == "N") {
      CMLS_DEST_BIN_REGN_CD_DRVD
    } else if (CMLS_DEST_BIN_REGN_CD.toInt > 0 && CMLS_DEST_BIN_REGN_CD.toInt <7) {
      CMLS_DEST_BIN_REGN_CD
    }
    else {
      "01"
    }
  })

  def gmbsBinRegnCdOutgoing = udf((CMLS_SRCE_OF_TRAN_FLG: String, CMLS_ACCT_REGN_CD_DRVD: String, cmls_xbrdr_ind: String, CMLS_SRCE_BIN_REGN_CD_DRVD: String, CMLS_SRCE_BIN_REGN_CD: String) =>
  {

    var gmbs_xbrdr_flg = if (cmls_xbrdr_ind == "Y" && CMLS_SRCE_OF_TRAN_FLG == "A") {
      cmls_xbrdr_ind
    } else {
      "N"
    }


    if (gmbs_xbrdr_flg == "N") {
      CMLS_SRCE_BIN_REGN_CD_DRVD
    } else if (CMLS_SRCE_BIN_REGN_CD.toInt > 0 && CMLS_SRCE_BIN_REGN_CD.toInt <7) {
      CMLS_SRCE_BIN_REGN_CD
    }
    else {
      "01"
    }

  })

  def gmbsBinRegnCdOutgoingOth = udf((CMLS_SRCE_OF_TRAN_FLG: String, CMLS_ACCT_REGN_CD_DRVD: String, cmls_xbrdr_ind: String, CMLS_SRCE_BIN_REGN_CD_DRVD: String, CMLS_SRCE_BIN_REGN_CD: String) =>
  {

    var gmbs_xbrdr_flg = if (cmls_xbrdr_ind == "Y" && CMLS_SRCE_OF_TRAN_FLG == "A") {
      cmls_xbrdr_ind
    } else {
      "N"
    }

    if (CMLS_SRCE_OF_TRAN_FLG == "A" || CMLS_ACCT_REGN_CD_DRVD == " " || CMLS_ACCT_REGN_CD_DRVD != "" || CMLS_ACCT_REGN_CD_DRVD == "00") {
      if (gmbs_xbrdr_flg == "N") {
        CMLS_SRCE_BIN_REGN_CD_DRVD
      } else {
        CMLS_SRCE_BIN_REGN_CD
      }
    } else if (gmbs_xbrdr_flg == "N") {
      CMLS_SRCE_BIN_REGN_CD_DRVD
    } else {
      CMLS_ACCT_REGN_CD_DRVD
    }

  })

  val gmbsBinOrigCtryCdIncoming = udf((CMLS_XBRDR_IND: String, cmls_dest_bin_ctry_cd_drvd: String, ctry_cd: String) =>
  {
    if (CMLS_XBRDR_IND.trim().equals("N")) {
      if (cmls_dest_bin_ctry_cd_drvd.equals("000")) {
        "840"
      } else {
        cmls_dest_bin_ctry_cd_drvd
      }
    } else {
      ctry_cd
    }
  })

  val gmbsBinOrigCtryCdOutgoing = udf((CMLS_XBRDR_IND: String, cmls_srce_bin_ctry_cd_drvd: String, ctry_cd: String) =>
  {
    if (CMLS_XBRDR_IND.trim().equals("N")) {
      if (cmls_srce_bin_ctry_cd_drvd.equals("000")) {
        "840"
      } else {
        cmls_srce_bin_ctry_cd_drvd
      }
    } else {
      ctry_cd
    }
  })

}
