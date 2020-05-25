package com.demo.bigdata.dw.agg

import com.demo.bigdata.utils._

/**
 * 进项分析按税务机关聚合 从DW_AGG_JXFX_NSR抽取数据到 dw_agg_jxfx_swjg
 * @author hada
 */
object JxfxSwjg {
  def main(args: Array[String]) {
  val task = LogUtils.start("jxfxSwjg")
  try {
    val spark = SparkUtils.init("jxfxSwjg")
    
    val result = spark.sql(
      " SELECT NSR.DATE_KEY DATE_KEY,NSR.GF_NSR_KEY GF_NSR_KEY,NSR.GF_SWJG_KEY GF_SWJG_KEY,"+
      " NSR.XF_SWJG_KEY XF_SWJG_KEY,NSR.FP_LB FP_LB,COUNT(DISTINCT NSR.XF_NSR_KEY) XFHS,SUM(NSR.JE) JE, "+
      " SUM(NSR.SE) SE,SUM(NSR.FPFS) FPFS "+
      " FROM dw.DW_AGG_JXFX_NSR NSR "+
      " GROUP BY NSR.DATE_KEY,NSR.GF_NSR_KEY, "+
      " NSR.GF_SWJG_KEY,NSR.XF_SWJG_KEY, NSR.FP_LB")
    
    DataFrameUtils.saveOverwrite(result, "dw", "dw_agg_jxfx_swjg")
    LogUtils.end(task)
    } catch { case ex:Exception  => LogUtils.error(task, ex) }
  }
}
