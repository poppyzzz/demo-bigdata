package com.demo.bigdata.etl.dzdz

import com.demo.bigdata.utils._

/**
 * 删除表JXFP
 * @author hada
 */
object DltJxfp {  
   def main(args: Array[String]){
    val task = LogUtils.start("DltJxfp")
    try {
    val spark = SparkUtils.init("DltJxfp")
    val result = spark.sql(" drop table etl.ETL_JXFP ")
    LogUtils.end(task)
    } catch { case ex:Exception  => LogUtils.error(task, ex) }    
  }  
  
}