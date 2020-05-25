package com.demo.bigdata.dw.bak

import com.demo.bigdata.utils._

object BakDwFactXxfp {
  def main(args: Array[String]): Unit = {
    val task = LogUtils.start("BakDwFactXxfp")
    try {
      val spark = SparkUtils.init("BakDwFactXxfp")
      
      val param = new UpdateParam()
      param.dataBase = "dw"
      param.table = "dw_fact_xxfp"
      param.partitionField = "datekey"
      param.partitionType = "bigint"
      param.partedField = "date_key"
      param.keyField = "xxfp_id"
      param.subtables = Array("dw_fact_xxfpmx")
      BackupUtils.updateDataToHistory(spark, param)
      
      LogUtils.end(task)
    } catch { case ex: Exception => LogUtils.error(task, ex) }
  }
  
}