package com.demo.bigdata.etl.dzdz

import com.demo.bigdata.utils._

/**
 * 机动车统一销售发票进入销项发票的ETL过程
 * @author Huangr
 */
object JdcfpXx {
  def main(args: Array[String]){
    val task = LogUtils.start("jdcfpXx")
    try {
      val spark = SparkUtils.init("jdcfpXx")
      val ETLFPMAPNUM = PropertyFile.getProperty("ETLFPMAPNUM").toInt
    
      spark.udf.register("getXxfpId",(fpdm:String, fphm:String, kprq:String) => if(null==kprq) fpdm+"X"+fphm+"X" else fpdm+"X"+fphm+"X"+kprq)
      
      UdfRegister.fillNsr(spark)
      UdfRegister.getSwjg(spark)
      UdfRegister.cutSL(spark)
      
      val result = spark.sql("SELECT getXxfpId(D.FPDM,D.FPHM,D.KPRQ) XXFP_ID,D.FPDM,D.FPHM,"+
          "'JD' FP_LB,case trim(D.fpzt_dm) when '0' then 'N' when '1' then 'N' else 'Y' end ZFBZ,D.ZFRQ as ZFSJ,D.JSHJ-D.ZZSSE JE,cast(cutSL(D.SLV) as double) SL,D.ZZSSE SE,"+
          "getSwjg(D.GF_QXSWJG_DM,fillNsr(D.GFSBH)) GF_SWJG_DM,NSR.SWJG_KEY XF_SWJG_DM,fillNsr(D.NSRSBH) XF_NSRSBH,"+
          "D.XHDWMC XF_NSRMC,fillNsr(D.GFSBH) GF_NSRSBH,"+
          "D.GHDW GF_NSRMC,D.KPRQ,0.0 BSYF,D.KPRQ BSSJ,'' SFSJWZ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') CZSJ,D.SKM,"+
          "'' SHRSBH,'' SHRMC,'' FHRSBH,'' FHRMC,'' QYD,'' SKPH,0.0 JSHJ,'' CZCH,'' CCDW,'' YSHWXX,'' BZ,'' BLBZ  "+
          "FROM dzdz.DZDZ_FPXX_JDCFP D "+
          "join dw.DW_DM_NSR NSR on D.NSRSBH = NSR.NSR_KEY "+
          " and NSR.WDBZ='1' ").repartition(ETLFPMAPNUM)
     
      DataFrameUtils.saveAppend(result, "etl", "etl_xxfp")
      LogUtils.end(task)
    } catch { case ex:Exception  => LogUtils.error(task, ex) }
  }
}