package com.demo.bigdata.dw.agg

import com.demo.bigdata.utils._

/**
 * 销项分析按纳税人聚合：从DW_AGG_XXFX_WP抽取数据到P_AGG_XXFX_NSR
 * @author Huangr
 */
object XxfxNsr {
  def main(args: Array[String]){
    val task = LogUtils.start("xxfxNsr")
    try {
      val spark = SparkUtils.init("xxfxNsr")
      UdfRegister.getDateKey(spark)
      UdfRegister.isFpdk(spark)
    
      val date_key = args(0)
      
      val factXxfpMx = PartitionUtils.getDfByPath(spark, "dw_bak", "dw_fact_xxfpmx", "datekey", date_key.toInt, date_key.toInt)
      factXxfpMx.createOrReplaceTempView("FACT_XXFPMX")
      val factXxfp = PartitionUtils.getDfByPath(spark, "dw_bak", "dw_fact_xxfp", "datekey", date_key.toInt, date_key.toInt)
      factXxfp.createOrReplaceTempView("FACT_XXFP")    
      
      val result = spark.sql(
          "     SELECT getDateKey(fp.KPRQ) DATE_KEY, fp.GF_NSRSBH GF_NSR_KEY,fp.GF_SWJG_KEY,"+
          "            NSR.NSRSBH XF_NSR_KEY,isFpdk(NSR.NSRSBH) DKBZ,fp.XF_SWJG_KEY, fp.FP_LB,"+
          "            SUM(mx.WP_SL) WP_SL,SUM(mx.JE) JE,SUM(mx.SE) SE,mx.SL,"+
          "            COUNT(DISTINCT fp.XXFP_ID) FPFS,fp.GF_NSRMC"+
          "       FROM FACT_XXFP fp "+
          " INNER JOIN FACT_XXFPMX mx ON fp.XXFP_ID = mx.XXFP_ID "+
          " INNER JOIN dw.DW_DM_NSR NSR ON fp.XF_NSRSBH = NSR.NSR_KEY"+
          "      WHERE fp.GF_NSRSBH is not null AND fp.GF_SWJG_KEY is not null "+
          "        AND fp.XF_NSRSBH is not null And fp.XF_SWJG_KEY is not null "+
          "        AND mx.QDBZ='00'"+
          "   GROUP BY getDateKey(fp.KPRQ),NSR.NSRSBH,fp.GF_SWJG_KEY,fp.GF_NSRSBH, "+
          "            fp.XF_SWJG_KEY,fp.FP_LB,mx.SL,fp.GF_NSRMC"
          )
          
      DataFrameUtils.saveOverwrite(result, "dw", "dw_agg_xxfx_nsr")
      LogUtils.end(task)
    } catch { case ex:Exception  => LogUtils.error(task, ex) }
  }    
  
}