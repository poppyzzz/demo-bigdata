package com.demo.bigdata.etl.dzdz

import com.demo.bigdata.utils._

/**
 * 删除表XXFP_QD
 * @author hada
 */
object DltXxfpQd {
   def main(args: Array[String]){
    val task = LogUtils.start("DltXxfpQd")
    try {
    val spark = SparkUtils.init("DltXxfpQd")
    val result = spark.sql(" drop table etl.ETL_XXFP_QD ")
    LogUtils.end(task)
    } catch { case ex:Exception  => LogUtils.error(task, ex) }    
  }  
  
}