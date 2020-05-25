package com.demo.bigdata.dw.agg

import com.demo.bigdata.utils._

/**
 * 进项分析按纳税人聚合 从etl.ETL_JXFP和etl.ETL_JXFP_QD抽取数据到 dw_agg_jxfx_nsr
 * @author hada
 */
object JxfxNsr {
  def main(args: Array[String]) {
    val task = LogUtils.start("jxfxNsr")
    try {
      val spark = SparkUtils.init("jxfxNsr")
            
      UdfRegister.getDateKey(spark)
      UdfRegister.isFpdk(spark)
      
      val date_key = args(0)
      
      val factJxfpMx = PartitionUtils.getDfByPath(spark, "dw_bak", "dw_fact_jxfpmx", "datekey", date_key.toInt, date_key.toInt)
      factJxfpMx.createOrReplaceTempView("FACT_JXFPMX")
      val factJxfp = PartitionUtils.getDfByPath(spark, "dw_bak", "dw_fact_jxfp", "datekey", date_key.toInt, date_key.toInt)
      factJxfp.createOrReplaceTempView("FACT_JXFP")

      val result = spark.sql(
          "     SELECT getDateKey(fp.kprq) date_key,nsr.nsrsbh gf_nsr_key,fp.gf_swjg_key,fp.xf_nsrsbh xf_nsr_key,"+
          "            isFpdk(fp.xf_nsrsbh) dkbz,fp.xf_swjg_key,fp.fp_lb,SUM(mx.wp_sl) wp_sl,SUM(mx.je) je,"+
          "            SUM(mx.se) se,mx.sl,COUNT(DISTINCT fp.jxfp_id) fpfs,fp.xf_nsrmc "+
          "       FROM FACT_JXFP fp "+
          " INNER JOIN FACT_JXFPMX mx ON fp.jxfp_id = mx.jxfp_id "+
          " INNER JOIN dw.dw_dm_nsr nsr ON fp.gf_nsrsbh = nsr.nsr_key"+
          "      WHERE fp.gf_nsrsbh IS NOT NULL AND fp.gf_swjg_key IS NOT NULL "+
          "        AND fp.xf_nsrsbh IS NOT NULL AND fp.xf_swjg_key IS NOT NULL AND mx.qdbz='00'"+
          "   GROUP BY getDateKey(fp.kprq),fp.xf_nsrsbh,fp.gf_swjg_key,nsr.nsrsbh,"+
          "            fp.xf_swjg_key,fp.fp_lb,fp.xf_nsrmc,mx.sl"
          ) 
      
      DataFrameUtils.saveOverwrite(result, "dw", "dw_agg_jxfx_nsr")
      LogUtils.end(task)
    } catch { case ex:Exception  => LogUtils.error(task, ex) }
  } 
}
