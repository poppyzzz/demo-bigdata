package com.demo.bigdata.etl.dzdz

import com.demo.bigdata.utils._

/**
 * 删除表JXFP_QD
 * @author hada
 */
object DltJxfpQd{
    def main(args: Array[String]){
    val task = LogUtils.start("DltJxfpQd")
    try {
    val spark = SparkUtils.init("DltJxfpQd")
    val result = spark.sql(" drop table etl.etl_JXFP_QD ")
    LogUtils.end(task)
    } catch { case ex:Exception  => LogUtils.error(task, ex) }     
  }  
}