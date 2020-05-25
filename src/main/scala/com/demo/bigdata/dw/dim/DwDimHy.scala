package com.demo.bigdata.dw.dim

import com.demo.bigdata.utils._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql._
import org.apache.spark.sql.types._

/**
 * DW行业表 从etl.etl_hymx抽取数据到dw.dw_dim_hy
 * @author Liruiming
 */
object DwDimHy {
  def main(args: Array[String]){
    val task = LogUtils.start("dwDimHy")
    try {
      val spark = SparkUtils.init("dwDimHy")
      spark.sql("drop table dw.dw_dim_hy")
      val result1 = spark.sql("select h.hy_dm hy_key,h.hy_dm,h.hy_mc,h.hy_dm hymx_dm,h.hy_mc hymx_mc,"+
        "h.yxbz,d.hydl_dm,d.hydl_mc,m.hyml_dm,m.hyml_mc,m.hyml_dm mldm,m.hyml_mc mlmc"+
        " from etl.etl_hy h join etl.etl_hydl d on h.hydl_dm = d.hydl_dm "+
        " join etl.etl_hyml m on d.hyml_dm = m.hyml_dm")
        
      DataFrameUtils.saveAppend(result1, "dw", "dw_dim_hy")
           
      val result = spark.sql("select mx.hymx_dm hy_key,hy.hy_dm,hy.hy_mc, mx.hymx_dm,"+
        "mx.hymx_mc,mx.yxbz,dl.hydl_dm,dl.hydl_mc,ml.hyml_dm,ml.hyml_mc,ml.hyml_dm mldm,ml.hyml_mc mlmc"+
        " from etl.etl_hymx mx join etl.etl_hy hy on mx.hy_dm = hy.hy_dm "+
	      " join etl.etl_hydl dl on mx.hydl_dm = dl.hydl_dm "+
	      " join etl.etl_hyml ml on dl.hyml_dm = ml.hyml_dm "+
	      " where mx.hymx_dm not like '%00'")
        
      DataFrameUtils.saveAppend(result, "dw", "dw_dim_hy")
      
      val list = List(Row("9999","9999","未知行业","9999","未知行业明细","Y","99","未知行业大类","9","未知行业门类","9","未知门类"))
   
      val schema = StructType(
           Array(
              StructField("hy_key", StringType, true),
              StructField("hy_dm", StringType, true),
              StructField("hy_mc", StringType, true),
              StructField("hymx_dm", StringType, true),
              StructField("hymx_mc", StringType, true),
              StructField("yxbz", StringType, true),
              StructField("hydl_dm", StringType, true),
              StructField("hydl_mc", StringType, true),
              StructField("hyml_dm", StringType, true),
              StructField("hyml_mc", StringType, true),
              StructField("mldm", StringType, true),
              StructField("mlmc", StringType, true)
           )
        )
      val sc = SparkUtils.getSparkConf("dwDimHy")
      val result2 = sc.parallelize(list)
      val sqlContext = new SQLContext(sc)
      val dataFrame = sqlContext.createDataFrame(result2, schema)
      DataFrameUtils.saveAppend(dataFrame, "dw", "dw_dim_hy")
      LogUtils.end(task)
    } catch { case ex:Exception  => LogUtils.error(task, ex) }
  }    
}