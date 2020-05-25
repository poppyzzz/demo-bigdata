package com.demo.bigdata.etl.dzdz

import com.demo.bigdata.utils._

/**
 * 增值税普通发票销项清单ETL程序：
 * 从增值税普通发票表(DZDZ_HWXX_PTFP)和销项发票表(ETL_JXFP)中抽取数据，
 * 进行集成、清洗、转换操作，插入ETL进项发票清单表(ETL_JXFP_QD)
 * @author chenyimeng
 */
object PtfpJxQd {
  def main(args: Array[String]) {
    val task = LogUtils.start("ptfpJxQd")
    try {
      val spark = SparkUtils.init("ptfpJxQd")
      val ETLFPMAPNUM = PropertyFile.getProperty("ETLFPMAPNUM").toInt
        
      spark.udf.register("getJxfpqdId", (fpdm:String, fphm:String, kprq:String,id:Double) =>if(null==kprq) fpdm+"X"+fphm+"X"+"null"+"X"+"00"+"X"+id else fpdm+"X"+fphm+"X"+kprq+"X"+"00"+"X"+id)
      spark.udf.register("getJxfpId", (fpdm:String, fphm:String, kprq:String) => if(null==kprq) fpdm+"X"+fphm+"X" else fpdm+"X"+fphm+"X"+kprq)
      UdfRegister.cutSL(spark)
  
        val result = spark.sql("SELECT getJxfpqdId(D.FPDM,D.FPHM,R.KPRQ,D.ID) JXFPQD_ID," +
          "getJxfpId(D.FPDM,D.FPHM,R.KPRQ) JXFP_ID ,D.ID HH,'P1' FP_LB,D.MC WP_MC," +
          "D.jldw WP_DW,D.GGXH WP_XH,D.SL WP_SL,D.DJ ,D.JE,cast(cutSL(D.SLV) as double) SL,D.SE," +
          "R.RZSJ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') CZSJ,R.KPRQ KPRQ,D.QDBZ," +
          " '' SKPH,'' SFZHM,'' CD,'' HGZS,'' JKZMSH,'' SJDH,'' FDJHM," +
          " '' CJHM,'' DH,'' ZH,'' KHYH,'' DW,'' XCRS,0.0 JSHJ, D.spbm " +
          " FROM dzdz.DZDZ_HWXX_PTFP D JOIN etl.ETL_JXFP R ON (D.FPDM = R.FPDM AND D.FPHM = R.FPHM)  and R.FP_LB='P1' ").repartition(ETLFPMAPNUM)

      DataFrameUtils.saveAppend(result, "etl", "etl_jxfp_qd")

      val result1 = spark.sql("SELECT getJxfpqdId(D.FPDM,D.FPHM,R.KPRQ,'1') JXFPQD_ID," +
        "getJxfpId(D.FPDM,D.FPHM,R.KPRQ) JXFP_ID ,1.0 HH,'P1' FP_LB,'无商品明细' WP_MC," +
        " '' WP_DW,'' WP_XH,1.0 WP_SL,D.DJ ,D.JE,cast(cutSL(R.SL) as double) SL,D.SE," +
        "R.RZSJ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') CZSJ,R.KPRQ ,'00' QDBZ," +
        " '' SKPH,'' SFZHM,'' CD,'' HGZS,'' JKZMSH,'' SJDH,'' FDJHM," +
        " '' CJHM,'' DH,'' ZH,'' KHYH,'' DW,'' XCRS,0.0 JSHJ, '9999999999999999999' spbm " +
        " FROM dzdz.DZDZ_HWXX_PTFP D JOIN etl.ETL_JXFP R ON (D.FPDM = R.FPDM AND D.FPHM = R.FPHM) " +
        " WHERE (D.FPDM is null or D.FPHM is null) and R.FP_LB='P1' ").repartition(ETLFPMAPNUM)
    
      DataFrameUtils.saveAppend(result, "etl", "etl_jxfp_qd")
      LogUtils.end(task)
    } catch { case ex:Exception  => LogUtils.error(task, ex) }
  }
}

