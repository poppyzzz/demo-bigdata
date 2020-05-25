package com.demo.bigdata.etl.dzdz

import com.demo.bigdata.utils._

/**
 * 增值税普通发票销项清单ETL程序：
 * 从增值税普通发票表(DZDZ_HWXX_PTFP)和销项发票表(ETL_XXFP)中抽取数据，
 * 进行集成、清洗、转换操作，插入ETL销项发票清单表(ETL_XXFP_QD)
 * @author Liruiming
 */
object PtfpXxQd {
  def main(args: Array[String]) {
    val task = LogUtils.start("ptfpXxQd")
    try {
      val spark = SparkUtils.init("ptfpXxQd")
      val ETLFPMAPNUM = PropertyFile.getProperty("ETLFPMAPNUM").toInt
        
      spark.udf.register("getXxfpqdId", (fpdm:String, fphm:String, kprq:String, qdbz:String, id:Double) => 
        if(null==kprq) fpdm+"X"+fphm+"X"+"null"+"X"+qdbz+"X"+id else fpdm+"X"+fphm+"X"+kprq+"X"+qdbz+"X"+id)
      spark.udf.register("getXxfpId", (fpdm:String, fphm:String, kprq:String) => if (null==kprq )fpdm+"X"+fphm+"X" else fpdm+"X"+fphm+"X"+kprq)
      UdfRegister.cutSL(spark)
    
      val result = spark.sql("SELECT getXxfpqdId(D.FPDM,D.FPHM,R.KPRQ,D.QDBZ,D.ID) XXFPQD_ID,"+
          "getXxfpId(D.FPDM,D.FPHM,R.KPRQ) XXFP_ID ,D.ID HH,'P1' FP_LB,D.MC WP_MC,D.GGXH WP_XH,D.JLDW WP_DW,"+
          "D.SL WP_SL,D.DJ,D.JE,cast(cutSL(D.SLV) as double) SL,D.SE,D.KPYF BSYF,R.BSSJ,"+
          "from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') CZSJ,D.FPDM,D.FPHM,'' WP_LB,R.KPRQ,D.QDBZ, "+
          "'' SKPH,'' SFZHM,'' CD,'' HGZS,'' JKZMSH,'' SJDH,'' FDJHM,"+
          "'' CJHM,'' DH,'' ZH,'' KHYH,'' DW,'' XCRS,R.JSHJ, D.spbm  "+
          "FROM dzdz.DZDZ_HWXX_PTFP D JOIN etl.ETL_XXFP R ON (D.FPDM = R.FPDM AND D.FPHM = R.FPHM) and R.FP_LB='P1'").repartition(ETLFPMAPNUM)
      
      DataFrameUtils.saveAppend(result, "etl", "etl_xxfp_qd")
    
    //fill a default record for these vouchers with no detail from resource data
    val result1 = spark.sql("SELECT getXxfpqdId(R.FPDM,R.FPHM,R.KPRQ,'00','1') XXFPQD_ID,"+
        "getXxfpId(R.FPDM,R.FPHM,R.KPRQ) XXFP_ID ,1.0 HH,'P1' FP_LB,'无商品明细' WP_MC,'' WP_XH,'' WP_DW,"+
        "1.0 WP_SL,R.JE DJ,R.JE,cast(cutSL(R.SL) as double) SL,R.SE,R.BSYF,R.BSSJ,"+
        "from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') CZSJ,R.FPDM,R.FPHM,'' WP_LB,R.KPRQ,'00' QDBZ, "+
        "'' SKPH,'' SFZHM,'' CD,'' HGZS,'' JKZMSH,'' SJDH,'' FDJHM,"+
        "'' CJHM,'' DH,'' ZH,'' KHYH,'' DW,'' XCRS,R.JSHJ,'9999999999999999999' spbm  "+
        "FROM dzdz.DZDZ_HWXX_PTFP D RIGHT JOIN etl.ETL_XXFP R ON (D.FPDM = R.FPDM AND D.FPHM = R.FPHM) "+
        "WHERE (D.FPDM is null or D.FPHM is null) and R.FP_LB='P1' ").repartition(ETLFPMAPNUM)
      
      DataFrameUtils.saveAppend(result, "etl", "etl_xxfp_qd")
   
      LogUtils.end(task)
    } catch { case ex:Exception  => LogUtils.error(task, ex) }
  }
}

