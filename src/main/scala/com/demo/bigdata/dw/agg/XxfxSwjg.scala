package com.demo.bigdata.dw.agg

import com.demo.bigdata.utils._

/**
 * 销项分析按税务机关聚合：从DW_AGG_XXFX_NSR抽取数据到P_AGG_XXFX_SWJG
 * @author Huangr
 */
object XxfxSwjg {
  def main(args: Array[String]){
    val task = LogUtils.start("xxfxSwjg")
    try {
    val spark = SparkUtils.init("xxfxSwjg")
           
    val result = spark.sql("SELECT NSR.DATE_KEY,NSR.GF_SWJG_KEY,NSR.XF_NSR_KEY,NSR.XF_SWJG_KEY,"+
        " NSR.FP_LB,"+
        " COUNT(DISTINCT NSR.GF_NSR_KEY) GFHS, COUNT(DISTINCT NSR.XF_NSR_KEY) XFHS, "+
        " SUM(NSR.JE) JE, SUM(NSR.SE) SE, SUM(NSR.FPFS) FPFS"+
        " FROM dw.DW_AGG_XXFX_NSR NSR "+
        " GROUP BY NSR.DATE_KEY,NSR.XF_NSR_KEY, NSR.XF_SWJG_KEY,NSR.GF_SWJG_KEY,NSR.FP_LB")
        
    DataFrameUtils.saveOverwrite(result, "dw", "dw_agg_xxfx_swjg")
    LogUtils.end(task)
    } catch { case ex:Exception => LogUtils.error(task, ex) }
  }      
}