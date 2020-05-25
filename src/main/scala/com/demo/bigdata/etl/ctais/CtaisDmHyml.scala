package com.demo.bigdata.etl.ctais

import com.demo.bigdata.utils._

/**
 * 抽取ctais库hyml
 * @author Huangr
 */
object CtaisDmHyml {
  def main(args: Array[String]){
    val task = LogUtils.start("ctaisDmHyml")
    try {
    val spark = SparkUtils.init("ctaisDmHyml")
   
    val result = spark.sql("SELECT D.hyml_dm,D.hyml_mc ,D.hyml_jc,D.yxbz,D.xybz "+
        "FROM ctais.dm_hyml D")
        
    DataFrameUtils.saveOverwrite(result, "etl", "etl_hyml")
     LogUtils.end(task)
    } catch { case ex:Exception  => LogUtils.error(task, ex) }
  }
}