package com.demo.bigdata.dw.fact

import com.demo.bigdata.utils._

/**
 * DW进项发票实时表 从etl.etl_jxfp抽取数据到dw.dw_fact_jxfp
 * @author Liruiming
 */
object DwFactJxfp {
  def main(args: Array[String]){
  val task = LogUtils.start("dwFactJxfp")
  try {
    val spark = SparkUtils.init("dwFactJxfp")
    UdfRegister.getDateKey(spark)
    UdfRegister.isFpdk(spark)
    UdfRegister.getTimestamp(spark)
    
    val result = spark.sql("SELECT D.JXFP_ID JXFP_KEY,D.GF_SWJG_DM GF_SWJG_KEY,D.XF_SWJG_DM XF_SWJG_KEY, "+
	      "getDateKey(D.KPRQ) DATE_KEY,D.GF_NSRSBH GF_NSR_KEY,D.XF_NSRSBH XF_NSR_KEY,D.FP_LB,"+
	      "D.JXFP_ID,D.FPDM,D.FPHM,D.JE,D.SL,D.SE,isFpdk(D.XF_NSRSBH) DKBZ,getTimestamp(D.RZSJ) RZRQ,"+
	      "D.XF_NSRMC,D.GF_NSRMC,D.CZSJ,D.XF_NSRSBH,D.GF_NSRSBH,getTimestamp(D.KPRQ) KPRQ,D.GF_SWJG_DM,D.XF_SWJG_DM,"+
        "D.SKM,D.SHRSBH,D.SHRMC,D.FHRSBH,D.FHRMC,D.QYD,D.SKPH,D.JSHJ,D.CZCH,D.CCDW,D.YSHWXX,D.BZ,D.TSPZBZ "+
        " FROM etl.etl_jxfp D WHERE (TRIM(D.ZFBZ)='N' or D.ZFBZ is null)").repartition(3)
    DataFrameUtils.saveOverwrite(result, "dw", "dw_fact_jxfp")
     
    //将数据存放到hdfs 供外部表读取
     val resultTemp = spark.sql("select jxfp_key,gf_swjg_key,xf_swjg_key,date_key,gf_nsr_key,"+
         " xf_nsr_key,fp_lb,jxfp_id,fpdm,fphm,sl,je,se,dkbz,rzrq,xf_nsrmc,gf_nsrmc,czsj,"+
         " xf_nsrsbh,gf_nsrsbh,kprq,rzyf,gf_swjg_dm,xf_swjg_dm,skm,shrsbh,shrmc,fhrsbh,"+
         " fhrmc,qyd,skph,jshj,czch,ccdw,yshwxx,bz from dw.dw_fact_jxfp")
		PartitionUtils.saveToHdfsPath(resultTemp, "dw_fact_jxfp_external")
        
    LogUtils.end(task)
    } catch { case ex:Exception => LogUtils.error(task, ex) }
  }    
}