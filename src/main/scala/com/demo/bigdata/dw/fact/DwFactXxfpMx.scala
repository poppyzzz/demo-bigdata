package com.demo.bigdata.dw.fact

import com.demo.bigdata.utils._

/**
 * 从ETL_XXFP_QD取数据到DW_FACT_XXFPMX
 * @author Huangr
 */
object DwFactXxfpMx {
  def main(args: Array[String]) {
  val task = LogUtils.start("dwFactXxfpMx")
  try {
    val spark = SparkUtils.init("dwFactXxfpMx")
    UdfRegister.getTimestamp(spark)
    
    val result = spark.sql("SELECT p.date_key,p.gf_nsr_key,p.xf_nsr_key,d.fp_lb,d.wp_mc,"+
        " d.wp_dw,d.wp_xh,d.wp_sl,d.dj,d.je,d.sl,d.se,d.fpdm,d.fphm,d.xxfp_id,p.zfbz,"+
        " getTimestamp(p.kprq) kprq,p.xf_nsrmc,p.gf_nsrmc,d.qdbz,getTimestamp(d.bssj) bssj,"+
        " d.hh,d.xxfpqd_id xxfpmx_id,p.xf_nsrsbh,p.gf_nsrsbh,d.xxfpqd_id xxfpmx_key,"+
        " p.gf_swjg_key,p.xf_swjg_key,p.DKBZ,d.wp_lb,d.skph,d.sfzhm,d.cd,d.hgzs,d.jkzmsh,"+
        " d.sjdh,d.fdjhm,d.cjhm,d.dh,d.zh,d.khyh,d.dw,d.xcrs,d.jshj,d.spbm"+
        " FROM etl.etl_xxfp_qd d JOIN dw.dw_fact_xxfp p ON d.xxfp_id = p.xxfp_id ")
    DataFrameUtils.saveOverwrite(result, "dw", "dw_fact_xxfpmx")
    
    //将数据存放到hdfs 供外部表读取
    val resultTemp = spark.sql("select date_key,gf_nsr_key,xf_nsr_key,fp_lb,wp_mc,wp_dw,wp_xh,"+
      "wp_sl,dj,je,sl,se,fpdm,fphm,xxfp_id,zfbz,kprq,xf_nsrmc,"+
      "gf_nsrmc,bssj,hh,xxfpmx_id,xf_nsrsbh,gf_nsrsbh,xxfpmx_key,"+
      "gf_swjg_key,xf_swjg_key,wp_lb,skph,sfzhm,cd,hgzs,jkzmsh,"+
      "sjdh,fdjhm,cjhm,dh,zh,khyh,dw,xcrs,jshj,qdbz,spbm,dkbz from dw.dw_fact_xxfpmx")
	  PartitionUtils.saveToHdfsPath(resultTemp, "dw_fact_xxfpmx_external")
	   
    LogUtils.end(task)
    } catch { case ex:Exception  => LogUtils.error(task, ex) }
  }  
}