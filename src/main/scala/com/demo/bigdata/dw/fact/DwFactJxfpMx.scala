package com.demo.bigdata.dw.fact

import com.demo.bigdata.utils._

/**
 * DW进项发票实时表 从etl.etl_jxfp_qd抽取数据到dw.dw_fact_jxfpmx
 * @author Liruiming
 */
object DwFactJxfpMx {
  def main(args: Array[String]){
  val task = LogUtils.start("dwFactJxfpMx")
  try {
    val spark = SparkUtils.init("dwFactJxfpMx")
    UdfRegister.getTimestamp(spark)   
     
    val result = spark.sql("SELECT D.JXFPQD_ID JXFPMX_KEY,jx.GF_SWJG_KEY ,jx.XF_SWJG_KEY,"+
        " jx.DATE_KEY,jx.GF_NSR_KEY,jx.XF_NSR_KEY,D.FP_LB,D.WP_MC,D.WP_DW,D.WP_XH,D.WP_SL,"+
        " D.DJ,D.JE,D.SL,D.SE,D.JXFP_ID,jx.FPDM,jx.FPHM,jx.XF_NSRMC,jx.GF_NSRMC,jx.DKBZ,"+
        " getTimestamp(jx.RZRQ) RZRQ,D.CZSJ,D.HH,D.JXFPQD_ID JXFPMX_ID,jx.XF_NSRSBH,"+
        " jx.GF_NSRSBH,D.QDBZ,D.SKPH,D.SFZHM,D.CD,D.HGZS,D.JKZMSH,D.SJDH,D.FDJHM,D.CJHM,"+
        " D.DH,D.ZH,D.KHYH,D.DW,D.XCRS,D.JSHJ,D.SPBM,getTimestamp(jx.KPRQ) KPRQ "+
        " FROM etl.etl_jxfp_qd D join dw.dw_fact_jxfp jx on D.JXFP_ID = jx.JXFP_ID ")
      DataFrameUtils.saveOverwrite(result, "dw", "dw_fact_jxfpmx")
   
      //将数据存放到hdfs 供外部表读取
      val resultTemp = spark.sql("select jxfpmx_key,gf_swjg_key,xf_swjg_key,date_key,gf_nsr_key,"+
        " xf_nsr_key,fp_lb,wp_mc,wp_dw,wp_xh,wp_sl,dj,je,sl,se,jxfp_id,fpdm,fphm,xf_nsrmc,"+
        " gf_nsrmc,rzrq,dkbz,czsj,hh,jxfpmx_id,xf_nsrsbh,gf_nsrsbh,qdbz,skph,sfzhm,cd,hgzs,"+
        " jkzmsh,sjdh,fdjhm,cjhm,dh,zh,khyh,dw,xcrs,jshj,kprq,spbm from dw.dw_fact_jxfpmx")
      PartitionUtils.saveToHdfsPath(resultTemp,"dw_fact_jxfpmx_external")
	            
    LogUtils.end(task)
    } catch { case ex:Exception  => LogUtils.error(task, ex) }
  }    
	


}