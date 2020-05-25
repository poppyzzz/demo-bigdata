package com.demo.bigdata.etl.ctais

import com.demo.bigdata.utils._

/**
 * 税务机关ETL程序，从ctais税务机关表(DM_SWJG)中抽取数据，进行集成、清洗、转换操作，插入ETL税务机关表(ETL_SWJG)
 * @author Liruiming
 */
object CtaisDmSwjg {
  def main(args: Array[String]){
    val task = LogUtils.start("ctaisDmSwjg")
    try {
    val spark = SparkUtils.init("ctaisDmSwjg")
   
    val result = spark.sql("SELECT D.swjg_dm,D.swjg_mc ,D.swjg_jc,D.swjg_bz,D.sj_swjg_dm,D.nsr_swjg_bz, "+
        "D.jgjc_dm,D.swjg_jg,D.swjg_yb,D.swjg_dz,D.swjg_dh "+
        "FROM ctais.dm_swjg D")
        
    DataFrameUtils.saveOverwrite(result, "etl", "etl_swjg")
    LogUtils.end(task)
    } catch { case ex:Exception  => LogUtils.error(task, ex) }
  }
}