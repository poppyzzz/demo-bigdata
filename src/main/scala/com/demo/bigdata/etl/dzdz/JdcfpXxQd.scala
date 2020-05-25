package com.demo.bigdata.etl.dzdz

import com.demo.bigdata.utils._

/**
 * 增值税机动车发票销项清单ETL程序：
 * 从增值税机动车普通发票表(DZDZ_FPXX_JDCFP)和销项发票表(ETL_XXFP)中抽取数据，
 * 进行集成、清洗、转换操作，插入ETL销项发票清单表(ETL_XXFP_QD)
 * @author Liruiming
 */
object JdcfpXxQd {
  def main(args: Array[String]) {
    val task = LogUtils.start("jdcfpXxQd")
    try {
      val spark = SparkUtils.init("jdcfpXxQd")
      val ETLFPMAPNUM = PropertyFile.getProperty("ETLFPMAPNUM").toInt
    
      spark.udf.register("getXxfpqdId", (fpdm:String, fphm:String, kprq:String) => 
        if(null==kprq) fpdm+"X"+fphm+"X"+"null"+"0000" else fpdm+"X"+fphm+"X"+kprq+"0000")
      spark.udf.register("getXxfpId", (fpdm:String, fphm:String, kprq:String) => if (null==kprq )fpdm+"X"+fphm+"X" else fpdm+"X"+fphm+"X"+kprq)
      spark.udf.register("getWpmc",(cpxh:String , cllx:String) => cpxh+cllx)
      UdfRegister.cutSL(spark)
    
    val result = spark.sql("SELECT getXxfpqdId(D.FPDM,D.FPHM,D.KPRQ) XXFPQD_ID,"+
        "getXxfpId(D.FPDM,D.FPHM,D.KPRQ) XXFP_ID ,1.0 HH,'JD' FP_LB,getWpmc(D.CPXH,D.CLLX) WP_MC,"+
        "D.CPXH WP_XH,'辆' WP_DW,1.0 WP_SL,D.CJFY DJ,D.JSHJ-D.ZZSSE JE,cast(cutSL(D.SLV) as double) SL,D.ZZSSE SE,"+
        "0.0 BSYF,D.KPRQ BSSJ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') CZSJ,D.FPDM,D.FPHM,"+
        "'' WP_LB,D.KPRQ,'00' QDBZ,'' SKPH,D.SFZHM,"+
        "D.CD,D.HGZS,D.JKZMSH,D.SJDH,D.FDJHM,D.CJHM,D.DH,D.ZH,D.KHYHZH KHYH,D.DW,D.XCRS,D.JSHJ, D.spbm  "+
        "FROM dzdz.DZDZ_FPXX_JDCFP D "+
        "join dw.DW_DM_NSR NSR on D.NSRSBH = NSR.NSR_KEY and NSR.WDBZ='1' ").repartition(ETLFPMAPNUM)
      
      DataFrameUtils.saveAppend(result, "etl", "etl_xxfp_qd")
      LogUtils.end(task)
    } catch { case ex:Exception  => LogUtils.error(task, ex) }
  }
}
