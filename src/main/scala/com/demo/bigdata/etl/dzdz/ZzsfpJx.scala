package com.demo.bigdata.etl.dzdz

import com.demo.bigdata.utils._

/**
 * ETL: 处理增值税进项发票
 * @author fangang
 */
object ZzsfpJx {
  def main(args: Array[String]): Unit = {
    val task = LogUtils.start("zzsfpJxQd")
    try {
      val spark = SparkUtils.init("zzsfpJx")
      val ETLFPMAPNUM = PropertyFile.getProperty("ETLFPMAPNUM").toInt
    
      spark.udf.register("getJxfpId", (fpdm:String, fphm:String, kprq:String) => if(null==kprq) fpdm+"X"+fphm+"X" else fpdm+"X"+fphm+"X"+kprq)
      UdfRegister.fillNsr(spark)
      UdfRegister.fillSwjg(spark)
      UdfRegister.cutSL(spark)
      
      val result = spark.sql("SELECT getJxfpId(D.FPDM,D.FPHM,D.KPRQ) JXFP_ID,D.FPDM,D.FPHM,"+
        "'YB' FP_LB,D.JE JE,cast(cutSL(D.SE/D.JE) as double) SL,D.SE SE,fillNsr(D.XFSBH) XF_NSRSBH,"+
        "D.XFMC XF_NSRMC,fillNsr(D.GFSBH) GF_NSRSBH,D.GFMC GF_NSRMC,D.KPRQ,"+
        " D.KPRQ RZSJ,D.XF_QXSWJG_DM SWJG_DM,"+
        " from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') CZSJ,NSR.SWJG_KEY GF_SWJG_DM,"+
        "getSwjg(D.XF_QXSWJG_DM,fillNsr(D.XFSBH)) XF_SWJG_DM,case trim(D.fpzt_dm) when '0' then 'N' when '1' then 'N' else 'Y' end ZFBZ,'' SKM, "+
        "'' SHRSBH,'' SHRMC,'' FHRSBH,'' FHRMC,'' QYD,'' SKPH,"+
        "D.JSHJ,'' CZCH,'' CCDW,'' YSHWXX,D.BZ,D.tspz_dm as TSPZBZ,CASE WHEN length(trim(D.zfrq))>15 THEN D.zfrq ELSE NULL END ZFSJ "+
        "FROM dzdz.DZDZ_FPXX_ZZSFP D JOIN DW.DW_DM_NSR NSR ON D.GFSBH = NSR.NSR_KEY and NSR.WDBZ='1' ").repartition(ETLFPMAPNUM)
      
      DataFrameUtils.saveAppend(result, "etl", "etl_jxfp")
      
      LogUtils.end(task)
    } catch { case ex:Exception  => LogUtils.error(task, ex) }
  }
}