package com.demo.bigdata.dw.dim

import com.demo.bigdata.utils._
import java.text.SimpleDateFormat
import org.apache.spark.sql._
import org.apache.spark.sql.types._

/**
 * DW_DIM_DATE 日期维表 
 * @author Liruiming
 */
object DwDimDate{
    def main(args: Array[String]){
      val task = LogUtils.start("dwDimDate")
      try {
        val sc = SparkUtils.getSparkConf("dwDimDate")
        val sdf = new SimpleDateFormat("yyyy-MM")
        val strStart = args.apply(0)
        val dateStart = sdf.parse(strStart)
        val strEnd = args.apply(1)
        val dateEnd = sdf.parse(strEnd)
        val list = List()
        val listDate = DateUtils.getMonthsBetween(dateStart,dateEnd,list)
        
        val rows = listDate.map { 
          x => Row(
              DateUtils.format(x, "yyyyMM").toLong, 
              DateUtils.format(x, "yyyy"),
              DateUtils.format(x, "MM"),
              DateUtils.format(x, "yyyyMM").toDouble,
              DateUtils.format(x, "yyyy年"),
              DateUtils.format(x, "MM月"),
              DateUtils.format(x, "yyyy年MM月")
            )}
       
         val schema = StructType(Array(
          StructField("date_key", LongType, false),
          StructField("nd", StringType, true),
          StructField("yf", StringType, true),
          StructField("ny", DoubleType, true),
          StructField("nd_mc", StringType, true),
          StructField("yf_mc", StringType, true),
          StructField("ny_mc", StringType, true)
            )
          )
        val result = sc.parallelize(rows)
        val sqlContext = new SQLContext(sc)
        val dataFrame = sqlContext.createDataFrame(result, schema)
        DataFrameUtils.saveOverwrite(dataFrame, "dw", "dw_dim_date")
        LogUtils.end(task)
    } catch { case ex:Exception  => LogUtils.error(task, ex) }
  }
}