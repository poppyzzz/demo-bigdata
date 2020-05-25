package com.demo.bigdata.dw.dim

import com.demo.bigdata.utils._

/**
 * DW税务机关级次表   从etl.etl_swjg抽取数据到dw.dw_dim_swjg_jc
 * @author Liruiming
 */
object DwDimSwjgJc {
  def main(args: Array[String]){
  val task = LogUtils.start("dwDimSwjgJc")
  try {
    val spark = SparkUtils.init("dwDimSwjgJc")
    spark.sql("drop table dw.dw_dim_swjg_jc")
    
    val result = spark.sql("select t.swjg_dm sjswjg_key,t.swjg_dm sjswjg_dm,"+
     " t.swjg_mc sjswjg_mc,"+
     " t.jgjc_dm sjswjg_jc,"+
     " t.swjg_bz sjswjg_bz,"+
     " t.swjg_dm xjswjg_key,"+
     " t.swjg_dm xjswjg_dm,"+
     " t.swjg_mc xjswjg_mc,"+
     " t.jgjc_dm xjswjg_jc,"+
     " t.swjg_bz xjswjg_bz,"+
     " '0' swjglevel from etl.etl_swjg t ").repartition(1)
      
    DataFrameUtils.saveAppend(result, "dw", "dw_dim_swjg_jc")
    
     val result1 = spark.sql("select t.swjg_dm sjswjg_key,t.swjg_dm sjswjg_dm,"+
     " t.swjg_mc sjswjg_mc,"+
     " t.jgjc_dm sjswjg_jc,"+
     " t.swjg_bz sjswjg_bz,"+
     " t1.swjg_dm xjswjg_key,"+
     " t1.swjg_dm xjswjg_dm,"+
     " t1.swjg_mc xjswjg_mc,"+
     " t1.jgjc_dm xjswjg_jc,"+
     " t1.swjg_bz xjswjg_bz,"+
     " '1' swjglevel "+
     " from etl.etl_swjg t, etl.etl_swjg t1  "+
     " where t.swjg_dm = t1.sj_swjg_dm  ").repartition(1)
      
    DataFrameUtils.saveAppend(result1, "dw", "dw_dim_swjg_jc")
    
    val result2 = spark.sql("select t.swjg_dm sjswjg_key,t.swjg_dm sjswjg_dm,"+
     " t.swjg_mc sjswjg_mc,"+
     " t.jgjc_dm sjswjg_jc,"+
     " t.swjg_bz sjswjg_bz,"+
     " t2.swjg_dm xjswjg_key,"+
     " t2.swjg_dm xjswjg_dm,"+
     " t2.swjg_mc xjswjg_mc,"+
     " t2.jgjc_dm xjswjg_jc,"+
     " t2.swjg_bz xjswjg_bz,"+
     " '2' swjglevel "+
     " from etl.etl_swjg t, etl.etl_swjg t1,etl.etl_swjg t2 "+
     " where t.swjg_dm = t1.sj_swjg_dm and t1.swjg_dm = t2.sj_swjg_dm ").repartition(1)
      
    DataFrameUtils.saveAppend(result2, "dw", "dw_dim_swjg_jc")
    
    val result3 = spark.sql("select t.swjg_dm sjswjg_key,t.swjg_dm sjswjg_dm,"+
     " t.swjg_mc sjswjg_mc,"+
     " t.jgjc_dm sjswjg_jc,"+
     " t.swjg_bz sjswjg_bz,"+
     " t3.swjg_dm xjswjg_key,"+
     " t3.swjg_dm xjswjg_dm,"+
     " t3.swjg_mc xjswjg_mc,"+
     " t3.jgjc_dm xjswjg_jc,"+
     " t3.swjg_bz xjswjg_bz,"+
     " '3' swjglevel "+
     " from etl.etl_swjg t, etl.etl_swjg t1,etl.etl_swjg t2,etl.etl_swjg t3 "+
     " where t.swjg_dm = t1.sj_swjg_dm and t1.swjg_dm = t2.sj_swjg_dm and "+
     " t2.swjg_dm = t3.sj_swjg_dm").repartition(1)
      
    DataFrameUtils.saveAppend(result3, "dw", "dw_dim_swjg_jc")
    
    val result4 = spark.sql("select t.swjg_dm sjswjg_key,t.swjg_dm sjswjg_dm,"+
     " t.swjg_mc sjswjg_mc,"+
     " t.jgjc_dm sjswjg_jc,"+
     " t.swjg_bz sjswjg_bz,"+
     " t4.swjg_dm xjswjg_key,"+
     " t4.swjg_dm xjswjg_dm,"+
     " t4.swjg_mc xjswjg_mc,"+
     " t4.jgjc_dm xjswjg_jc,"+
     " t4.swjg_bz xjswjg_bz,"+
     " '4' swjglevel "+
     " from etl.etl_swjg t, etl.etl_swjg t1,etl.etl_swjg t2,etl.etl_swjg t3 ,etl.etl_swjg t4 "+
     " where t.swjg_dm = t1.sj_swjg_dm and t1.swjg_dm = t2.sj_swjg_dm and t2.swjg_dm = t3.sj_swjg_dm "+
     " and t3.swjg_dm = t4.sj_swjg_dm").repartition(1)
      
    DataFrameUtils.saveAppend(result4, "dw", "dw_dim_swjg_jc")
    
    val result5 = spark.sql("select t.swjg_dm sjswjg_key,t.swjg_dm sjswjg_dm,"+
     " t.swjg_mc sjswjg_mc,"+
     " t.jgjc_dm sjswjg_jc,"+
     " t.swjg_bz sjswjg_bz,"+
     " t5.swjg_dm xjswjg_key,"+
     " t5.swjg_dm xjswjg_dm,"+
     " t5.swjg_mc xjswjg_mc,"+
     " t5.jgjc_dm xjswjg_jc,"+
     " t5.swjg_bz xjswjg_bz,"+
     " '5' swjglevel "+
     " from etl.etl_swjg t, etl.etl_swjg t1,etl.etl_swjg t2,etl.etl_swjg t3 ,etl.etl_swjg t4,etl.etl_swjg t5 "+
     " where t.swjg_dm = t1.sj_swjg_dm and t1.swjg_dm = t2.sj_swjg_dm and t2.swjg_dm = t3.sj_swjg_dm "+
" and t3.swjg_dm = t4.sj_swjg_dm and t4.swjg_dm = t5.sj_swjg_dm").repartition(1)
      
    DataFrameUtils.saveAppend(result5, "dw", "dw_dim_swjg_jc")
    
    val result6 = spark.sql("select t.swjg_dm sjswjg_key,t.swjg_dm sjswjg_dm,"+
     " t.swjg_mc sjswjg_mc,"+
     " t.jgjc_dm sjswjg_jc,"+
     " t.swjg_bz sjswjg_bz,"+
     " t6.swjg_dm xjswjg_key,"+
     " t6.swjg_dm xjswjg_dm,"+
     " t6.swjg_mc xjswjg_mc,"+
     " t6.jgjc_dm xjswjg_jc,"+
     " t6.swjg_bz xjswjg_bz,"+
     " '6' swjglevel "+
     " from etl.etl_swjg t, etl.etl_swjg t1,etl.etl_swjg t2,etl.etl_swjg t3 ,etl.etl_swjg t4,etl.etl_swjg t5,"+
     " etl.etl_swjg t6 "+
     " where t.swjg_dm = t1.sj_swjg_dm and t1.swjg_dm = t2.sj_swjg_dm and t2.swjg_dm = t3.sj_swjg_dm "+
     " and t3.swjg_dm = t4.sj_swjg_dm and t4.swjg_dm = t5.sj_swjg_dm and "+
     " t5.swjg_dm = t6.sj_swjg_dm").repartition(1)
      
    DataFrameUtils.saveAppend(result6, "dw", "dw_dim_swjg_jc")
    
    val result7 = spark.sql("select t.swjg_dm sjswjg_key,t.swjg_dm sjswjg_dm,"+
     " t.swjg_mc sjswjg_mc,"+
     " t.jgjc_dm sjswjg_jc,"+
     " t.swjg_bz sjswjg_bz,"+
     " t7.swjg_dm xjswjg_key,"+
     " t7.swjg_dm xjswjg_dm,"+
     " t7.swjg_mc xjswjg_mc,"+
     " t7.jgjc_dm xjswjg_jc,"+
     " t7.swjg_bz xjswjg_bz,"+
     " '7' swjglevel "+
     " from etl.etl_swjg t, etl.etl_swjg t1,etl.etl_swjg t2,etl.etl_swjg t3 ,etl.etl_swjg t4,etl.etl_swjg t5,"+
     " etl.etl_swjg t6 ,etl.etl_swjg t7 "+
     " where t.swjg_dm = t1.sj_swjg_dm and t1.swjg_dm = t2.sj_swjg_dm and t2.swjg_dm = t3.sj_swjg_dm "+
     " and t3.swjg_dm = t4.sj_swjg_dm and t4.swjg_dm = t5.sj_swjg_dm "+
     " and t5.swjg_dm = t6.sj_swjg_dm and t6.swjg_dm = t7.sj_swjg_dm").repartition(1)
      
    DataFrameUtils.saveAppend(result7, "dw", "dw_dim_swjg_jc")
    LogUtils.end(task)
    } catch { case ex:Exception  => LogUtils.error(task, ex) } 
  }    
  

}