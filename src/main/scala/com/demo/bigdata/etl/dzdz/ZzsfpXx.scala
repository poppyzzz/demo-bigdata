package com.demo.bigdata.etl.dzdz

import com.demo.bigdata.utils._

/**
 * 抽取电子底账数据,增值税销项发票DZDZ_FPXX_ZZSFP抽取到ETL_XXFP
 * @author Huangr 
 */
object ZzsfpXx {
  def main(args: Array[String]){
    val task = LogUtils.start("zzsfpXx")
    try {
    val spark = SparkUtils.init("zzsfpXx")
    val ETLFPMAPNUM = PropertyFile.getProperty("ETLFPMAPNUM").toInt
    
    spark.udf.register("getXxfpId",(fpdm:String, fphm:String,  kprq:String) => if(null==kprq) fpdm+"X"+fphm+"X" else fpdm+"X"+fphm+"X"+kprq)
    
    UdfRegister.fillNsr(spark)
    UdfRegister.getSwjg(spark)
    UdfRegister.cutSL(spark)

    val result = spark.sql("SELECT getXxfpId(D.FPDM,D.FPHM,D.KPRQ) XXFP_ID,D.FPDM,D.FPHM,"+
        "'YB' FP_LB,case trim(D.fpzt_dm) when '0' then 'N' when '1' then 'N' else 'Y' end ZFBZ,D.zfrq as zfsj,D.JE JE,cast(cutSL(D.SE/D.JE) as double) SL,D.SE SE,"+
        " getSwjg(D.GF_QXSWJG_DM,fillNsr(D.GFSBH)) GF_SWJG_DM,NSR.SWJG_KEY XF_SWJG_DM ,fillNsr(D.XFSBH) XF_NSRSBH,"+
        "D.XFMC XF_NSRMC,fillNsr(D.GFSBH) GF_NSRSBH,D.GFMC GF_NSRMC,D.KPRQ,'' BSSJ,'' SFSJWZ,"+
        " from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') CZSJ,'' SKM,"+
        "'' SHRSBH,'' SHRMC,'' FHRSBH,'' FHRMC,'' QYD,'' SKPH,D.JSHJ,'' CZCH,'' CCDW,'' YSHWXX,D.BZ,'' BLBZ,D.tspz_dm as TSPZBZ "+
        s" FROM dzdz.DZDZ_FPXX_ZZSFP D "+
        "join dw.DW_DM_NSR NSR on D.XFSBH = NSR.NSR_KEY and NSR.WDBZ='1' ").repartition(ETLFPMAPNUM)
    
    DataFrameUtils.saveAppend(result, "etl", "etl_xxfp")
    LogUtils.end(task)
    } catch { case ex:Exception  => LogUtils.error(task, ex) }
  }
}