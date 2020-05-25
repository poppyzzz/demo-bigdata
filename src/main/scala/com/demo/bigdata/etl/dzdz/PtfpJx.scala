package com.demo.bigdata.etl.dzdz

import com.demo.bigdata.utils._

/**
 * 增值税普通发票销项ETL程序：
 * 从增值税普通发票表(DZDZ_FPJX_PTFP)中抽取数据，进行集成、清洗、转换操作，插入ETL进项发票表(ETL_JXFP)
 * @author chenyimeng
 */
object PtfpJx {
  def main(args: Array[String]) {
    val task = LogUtils.start("ptfpJx")
    try {
      val spark = SparkUtils.init("ptfpJx")
      val ETLFPMAPNUM = PropertyFile.getProperty("ETLFPMAPNUM").toInt
        
      spark.udf.register("getJxfpId",(fpdm:String, fphm:String, kprq:String) => if(null==kprq) fpdm+"X"+fphm+"X" else fpdm+"X"+fphm+"X"+kprq)
      spark.udf.register("getDouble", (str:String)=> if(null==str) "0".toDouble  else str.toDouble)
      spark.udf.register("getDoubleByDi", (str1:String,str2:String)=> if(null==str1 || null==str2) "0".toDouble else str1.toDouble/str2.toDouble)
      UdfRegister.fillNsr(spark)
      UdfRegister.getSwjg(spark)
      UdfRegister.cutSL(spark)
      
      val result = spark.sql("SELECT getJxfpId(D.FPDM,D.FPHM,D.KPRQ) JXFP_ID,D.FPDM,D.FPHM,"+
          "'P1' FP_LB,case trim(D.fpzt_dm) when '0' then 'N' when '1' then 'N' else 'Y' end ZFBZ,D.JE JE,cast(cutSL(D.SE/D.JE) as double) SL,D.SE SE,"+
          "getSwjg(D.XF_QXSWJG_DM,fillNsr(D.XFSBH)) XF_SWJG_DM,NSR.SWJG_KEY GF_SWJG_DM,"+
          "fillNsr(D.XFSBH) XF_NSRSBH,D.XFMC XF_NSRMC,fillNsr(D.GFSBH) GF_NSRSBH,"+
          "D.GFMC GF_NSRMC,D.KPRQ,0.0 BSYF,'' BSSJ,'' SFSJWZ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') CZSJ,"+
          "'' SKM,'' SHRSBH,'' SHRMC,'' FHRSBH,'' FHRMC,'' QYD,'' SKPH,0.0 JSHJ,"+
          "'' CZCH,'' CCDW,'' YSHWXX,'' BZ ,D.tspz_dm as TSPZBZ,CASE WHEN length(trim(D.zfrq))>15 THEN D.ZFrq ELSE NULL END ZFSJ "+
          "FROM dzdz.DZDZ_FPXX_PTFP D "+
          "join dw.DW_DM_NSR NSR on D.GFSBH = NSR.NSR_KEY and NSR.WDBZ='1' AND D.tspz_dm='02'").repartition(ETLFPMAPNUM)
      
      DataFrameUtils.saveAppend(result, "etl", "etl_jxfp")
      LogUtils.end(task)
    } catch { case ex:Exception  => LogUtils.error(task, ex) }
  }
}

