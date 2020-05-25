package com.demo.bigdata.etl.ctais

import com.demo.bigdata.utils._

/**
 * 抽取ctais库hydl
 * @author Huangr
 */
object CtaisDmHydl {
  def main(args: Array[String]){
    val task = LogUtils.start("ctaisDmHydl")
    try {
    val spark = SparkUtils.init("ctaisDmHydl")
   
 	val result = spark.sql("SELECT D.hydl_dm,D.hydl_mc ,D.hydl_jc,D.hyml_dm,D.yxbz,D.xybz "+
        "FROM ctais.dm_hydl D")
        
    DataFrameUtils.saveOverwrite(result, "etl", "etl_hydl")
    LogUtils.end(task)
    } catch { case ex:Exception  => LogUtils.error(task, ex) }
  }
}