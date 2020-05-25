package com.demo.bigdata.etl.dzdz

import org.apache.spark.sql.{SparkSession, SaveMode}
import java.text.DecimalFormat; 

/**
 * @author fangang
 */
object ZzsfpJxDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
    .builder()
    .config("spark.sql.warehouse.dir","")
    .enableHiveSupport()
    .appName("ZzsfpJx")
    .getOrCreate()
    
    spark.udf.register("getJxfpId", (fpdm:String, fphm:String, kprq:String) => if(null==kprq) fpdm+"X"+fphm+"X" else fpdm+"X"+fphm+"X"+kprq)
    
    spark.udf.register("fillNsr", (nsr:String) => if(nsr==null||nsr.trim().equals("")) "X99999999999999999" else nsr)
    
    spark.udf.register("fillSwjg", (swjg:String) => 
        if(null==swjg||"NULL"==swjg||"null"==swjg||swjg.trim().equals("")) "X9999999999" else 
          if(swjg.length()!=11) (swjg+"000000000000").substring(0,11) else swjg
      )
      
    spark.udf.register("concatSwjg", (swjg:String,gf_nsrsbh:String) => 
        if(null==swjg||"NULL"==swjg||"null"==swjg||swjg.trim().equals("") && 
            null==gf_nsrsbh||"NULL"==gf_nsrsbh||"null"==gf_nsrsbh||gf_nsrsbh.trim().equals("")) "X9999999999" else 
              if(null==swjg||"NULL"==swjg||"null"==swjg||swjg.trim().equals("")) "1"+gf_nsrsbh.substring(1,4)+"000000" else
          if(swjg.length()!=11) (swjg+"000000000000").substring(0,11) else swjg
      )
    
    val df_3   = new DecimalFormat("######0.000") 
    val df_2   = new DecimalFormat("######0.00")
    spark.udf.register("cutSL", (sl:Double)=> if(sl<=0.015) df_3.format(sl).toDouble else df_2.format(sl).toDouble)
    
    val result = spark.sql("SELECT getJxfpId(D.FPDM,D.FPHM,D.KPRQ) JXFP_ID,D.FPDM,D.FPHM,"+
      "'YB' FP_LB,D.JE JE,cast(cutSL(D.SE/D.JE) as double) SL,D.SE SE,fillNsr(D.XFSBH) XF_NSRSBH,"+
      "D.XFMC XF_NSRMC,fillNsr(D.GFSBH) GF_NSRSBH,D.GFMC GF_NSRMC,D.KPRQ,"+
      " D.KPRQ RZSJ,D.XF_QXSWJG_DM SWJG_DM,"+
      " from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') CZSJ,NSR.SWJG_KEY GF_SWJG_DM,"+
      "getSwjg(D.XF_QXSWJG_DM,fillNsr(D.XFSBH)) XF_SWJG_DM,case trim(D.fpzt_dm) when '0' then 'N' when '1' then 'N' else 'Y' end ZFBZ,'' SKM, "+
      "'' SHRSBH,'' SHRMC,'' FHRSBH,'' FHRMC,'' QYD,'' SKPH,"+
      "D.JSHJ,'' CZCH,'' CCDW,'' YSHWXX,D.BZ,D.tspz_dm as TSPZBZ,CASE WHEN length(trim(D.zfrq))>15 THEN D.zfrq ELSE NULL END ZFSJ "+
      "FROM dzdz.DZDZ_FPXX_ZZSFP D JOIN DW.DW_DM_NSR NSR ON D.GFSBH = NSR.NSR_KEY and NSR.WDBZ='1' ").repartition(3)
    
    result.write.mode(SaveMode.Append).saveAsTable("etl_jxfp")
  }
}