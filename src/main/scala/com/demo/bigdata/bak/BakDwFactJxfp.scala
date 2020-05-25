package com.demo.bigdata.dw.bak

import com.demo.bigdata.utils._

object BakDwFactJxfp {
  def main(args: Array[String]): Unit = {
    val task = LogUtils.start("BakDwFactJxfp")
    try {
      val spark = SparkUtils.init("BakDwFactJxfp")
      
      val param = new UpdateParam()
      param.dataBase = "dw"
      param.table = "dw_fact_jxfp"
      param.partitionField = "datekey"
      param.partitionType = "bigint"
      param.partedField = "date_key"
      param.keyField = "jxfp_id"
      param.subtables = Array("dw_fact_jxfpmx")
      BackupUtils.updateDataToHistory(spark, param)
      
      LogUtils.end(task)
    } catch { case ex: Exception => LogUtils.error(task, ex) }
  }
  
}