package com.demo.bigdata.etl.ctais

import com.demo.bigdata.utils._

/**
 * 抽取ctais库DJ_NSRXX到ETL_NSRXX
 * @author Huangr
 */
object CtaisNsrxx {
  def main(args: Array[String]){
    val task = LogUtils.start("ctaisNsrxx")
    try {
    val spark = SparkUtils.init("ctaisNsrxx")
        
    val result = spark.sql("SELECT XX.NSRDZDAH, XX.WSPZXH, XX.NSRSBH, XX.NSRBM, XX.NSRMC, XX.FDDBRMC, XX.FRZJLX_DM,XX.ZJHM,"+
        "LX.FRZJLX_MC FRZJLX_MC,XX.SCJYDZ, XX.BSRMC, XX.DHHM, XX.LSGX_DM, XX.HY_DM, XX.DJZCLX_DM, "+
        "XX.HZDJRQ, XX.QYKJZD_DM, XX.SWDJBLX_DM, XX.SWDJZLX_DM, XX.NSRZT_DM, XX.ZGSWRY_DM, XX.JDXZ_DM, XX.ZZSNSRLX_DM, "+
        "XX.GSYLB_DM, XX.NSRXYDJ_DM, XX.QYGM_DM, XX.CYLX_DM, XX.XZQH_YSFPBL_DM, XX.HGDM, XX.ZJG_BZ, XX.NSR_SWJG_DM,"+
        "XX.SWJG_DM, XX.LRR_DM, XX.LRRQ, XX.XGR_DM, XX.XGRQ, XX.GSZGSWJG_J_DM, XX.LSSWDJLX_DM, XX.DWXZ_DM, XX.GGHBZ "+
        " FROM ctais.DJ_NSRXX XX LEFT JOIN ctais.DM_FRZJLX LX ON trim(XX.FRZJLX_DM) = LX.FRZJLX_DM")
        
    DataFrameUtils.saveOverwrite(result, "etl", "etl_nsrxx") 
    LogUtils.end(task)
    } catch { case ex:Exception  => LogUtils.error(task, ex) }
  }
}