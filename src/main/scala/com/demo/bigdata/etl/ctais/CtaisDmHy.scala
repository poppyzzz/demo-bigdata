package com.demo.bigdata.etl.ctais

import com.demo.bigdata.utils._

/**
 * 行业ETL程序，从ctais行业表(DM_HY)中抽取数据，进行集成、清洗、转换操作，插入ETL行业表(ETL_HY)
 * @author Huangr
 */
object CtaisDmHy {
  def main(args: Array[String]){
    val task = LogUtils.start("ctaisDmHy")
    try {
    val spark = SparkUtils.init("ctaisDmHy")
   
    val result = spark.sql("SELECT D.hy_dm,D.hy_mc ,D.hy_jc,D.hydl_dm,D.xybz,D.yxbz "+
        "FROM ctais.dm_hy D")
        
    DataFrameUtils.saveOverwrite(result, "etl", "etl_hy")
     LogUtils.end(task)
    } catch { case ex:Exception  => LogUtils.error(task, ex) }
  }
}