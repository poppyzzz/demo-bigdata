package com.demo.bigdata.etl.ctais

import com.demo.bigdata.utils._

/**
 *行业明细ETL程序，从ctais行业明细表(DM_HYMX)中抽取数据，进行集成、清洗、转换操作，插入ETL行业明细表(ETL_HYMX)
 * @author Huangr
 */
object CtaisDmHymx {
  def main(args: Array[String]){
    val task = LogUtils.start("ctaisDmHymx")
    try {
    val spark = SparkUtils.init("ctaisDmHymx")
    spark.udf.register("fillZero", (dm:String)=> dm+"00")
   
    val result = spark.sql("SELECT D.hymx_dm,D.hymx_mc ,D.hymx_jc,D.hydl_dm,D.yxbz,D.xybz, fillZero(D.hydl_dm) hy_dm "+
        "FROM ctais.dm_hymx D")
        
    DataFrameUtils.saveOverwrite(result, "etl", "etl_hymx")
    LogUtils.end(task)
    } catch { case ex:Exception  => LogUtils.error(task, ex) }
  }
}