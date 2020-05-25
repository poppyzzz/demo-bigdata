package com.demo.bigdata.etl.dzdz

import com.demo.bigdata.utils._

/**
 * 增值税机动车发票进项清单ETL程序：
 * 从增值税机动车普通发票表(DZDZ_FPXX_JDCFP)中抽取数据，进行集成、清洗、转换操作，插入ETL进项发票清单表(ETL_JXFP_QD)
 * @author Liruiming
 */
object JdcfpJxQd {
  def main(args: Array[String]) {
    val task = LogUtils.start("jdcfpJxQd")
    try {
      val spark = SparkUtils.init("jdcfpJxQd")
      val ETLFPMAPNUM = PropertyFile.getProperty("ETLFPMAPNUM").toInt
    
    spark.udf.register("getJxfpqdId", (fpdm:String, fphm:String, kprq:String) => if(null==kprq)fpdm+"X"+fphm+"X"+"null"+"X"+"0000"
    else  fpdm+"X"+fphm+"X"+kprq+"X"+"0000")
    spark.udf.register("getJxfpId", (fpdm:String, fphm:String, kprq:String) => if(null==kprq)fpdm+"X"+fphm+"X" else fpdm+"X"+fphm+"X"+kprq)
    UdfRegister.cutSL(spark)
    
    val result = spark.sql("SELECT getJxfpqdId(D.FPDM,D.FPHM,D.KPRQ) JXFPQD_ID,"+
        "getJxfpId(D.FPDM,D.FPHM,D.KPRQ) JXFP_ID ,1.0 HH,'JD' FP_LB,D.CPXH WP_MC,"+
        "'辆' WP_DW,D.CLLX WP_XH,1.0 WP_SL,D.CJFY DJ,D.JSHJ-D.ZZSSE JE,cast(cutSL(D.SLV) as double) SL,"+
        "D.ZZSSE SE,D.KPRQ RZSJ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') CZSJ,D.KPRQ,"+
        "'00' QDBZ,'' SKPH,D.SFZHM,D.CD,D.HGZS,D.JKZMSH,D.SJDH,D.FDJHM,D.CJHM,D.DH,"+
        "D.ZH,D.KHYHZH as KHYH,D.DW,D.XCRS,D.JSHJ, D.spbm  "+
        "FROM dzdz.DZDZ_FPXX_JDCFP D "+
        "JOIN DW.DW_DM_NSR NSR ON D.GFSBH = NSR.NSR_KEY and NSR.WDBZ='1' ").repartition(ETLFPMAPNUM)
    
      DataFrameUtils.saveAppend(result, "etl", "etl_jxfp_qd")
      LogUtils.end(task)
    } catch { case ex:Exception  => LogUtils.error(task, ex) }
  }
}
