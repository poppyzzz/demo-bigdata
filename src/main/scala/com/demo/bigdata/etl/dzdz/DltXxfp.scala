package com.demo.bigdata.etl.dzdz

import com.demo.bigdata.utils._

/**
 * 删除表XXFP
 * @author hada
 */
object DltXxfp {
   def main(args: Array[String]){
    val task = LogUtils.start("DltXxfp")
    try {
    val spark = SparkUtils.init("DltXxfp")
    val result = spark.sql(" drop table etl.ETL_XXFP ")
    LogUtils.end(task)
    } catch { case ex:Exception  => LogUtils.error(task, ex) }    
  }  
  
}