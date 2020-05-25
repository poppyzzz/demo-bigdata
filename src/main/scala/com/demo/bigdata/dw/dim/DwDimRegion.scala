package com.demo.bigdata.dw.dim

import com.demo.bigdata.utils._

/**
 * 地区维表, 修改对应省份的外地标志, 并且补填本省swjg 到region
 * @author TangChenyang
 */
object DwDimRegion {
  def main(args: Array[String]) {
    val task = LogUtils.start("dwDimRegion")
    try {
      val spark = SparkUtils.init("dwDimRegion")
      val sfdm = PropertyFile.getProperty("ShengFenDM")

      val tsdqdm = spark.sql(s"SELECT tsdqdm FROM dw.dw_dim_sfdm WHERE sfdm = '$sfdm'").first.mkString

      // 创建原有地区维度表, 后面的过程将从此表取数, dw.dw_dim_region 将被覆盖
      spark.sql(
          " CREATE TABLE dw.dw_dim_region_orig AS "+
          " SELECT region_key, sjmc, sjdm, bjmc, bjdm, wdbz FROM dw.dw_dim_region")
             
       // region_original 原有的地区维表  region   
      val region_orig = spark.sql(
          " SELECT r.region_key, r.sjmc, r.sjdm, r.bjmc, r.bjdm, r.wdbz" +
          " FROM dw.dw_dim_region_orig r ")
      region_orig.createTempView("region_orig")

      /* region_swjg 用税务机关级次表维护地区维表 region
       * 取只有一层级次关系的税务机关数据 (swjglevel = 1)
       */
      val region_swjg = spark.sql(
          " SELECT  a.region_key, a.sjdm, SUBSTR(sj.swjg_mc,0,length(sj.swjg_mc)-5) AS sjmc," +
          "         a.bjdm, bj.swjg_mc AS bjmc,a.wdbz                                 " +
          " FROM (  SELECT SUBSTR(t.xjswjg_dm,2,6) AS region_key,                     " +
          "                SUBSTR(t.sjswjg_dm,2,4) AS sjdm,                           " +
          "                CONCAT('1',SUBSTR(t.sjswjg_dm,2,4),'000000') AS sjswjgdm,  " +
          "                SUBSTR(t.xjswjg_dm,2,6) AS bjdm,                           " +
          "                CONCAT('1',SUBSTR(t.xjswjg_dm,2,6),'0000') AS bjswjgdm,    " +
          "                '1' AS wdbz                                                  " +
          "         FROM dw.dw_dim_swjg_jc t                                          " +
         s"         WHERE SUBSTR(t.xjswjg_dm,2,2) LIKE '$sfdm%'                       " +
          "         AND swjglevel = 1                                                 " +
          "         AND SUBSTR(t.sjswjg_dm,2,6) <> SUBSTR(t.xjswjg_dm,2,6)            " +
          "         GROUP BY SUBSTR(t.xjswjg_dm,2,6), SUBSTR(t.sjswjg_dm,2,4)  ) a    " +
          " JOIN dw.dw_dim_swjg sj ON a. sjswjgdm = sj.swjg_dm                        " +
          " JOIN dw.dw_dim_swjg bj ON a. bjswjgdm = bj.swjg_dm                        ")

      region_swjg.createTempView("region_swjg")

      val region_inc = spark.sql(
          " SELECT s.region_key, s.sjmc, s.sjdm, s.bjmc, s.bjdm, s.wdbz " +
          " FROM region_swjg s LEFT JOIN region_orig o on s.region_key = o.region_key "+
          " WHERE o.region_key IS NULL"
          )
      // 原有的加上增量的
      val region_full_rdd = region_orig.rdd.++(region_inc.rdd)
      val region_full = spark.createDataFrame(region_full_rdd, region_orig.schema)
      region_full.createTempView("region_full")
      
      // 修改 region 外地标志 
      val result = spark.sql(
          " SELECT r.region_key, r.sjmc, r.sjdm, r.bjmc, r.bjdm," +
         s"        CASE WHEN r.region_key LIKE '$sfdm%' AND SUBSTR(r.region_key,1,4) NOT IN ($tsdqdm) then '1' " +
          "             ELSE '0' END wdbz" +
          " FROM region_full r ")

      DataFrameUtils.saveOverwrite(result, "dw", "dw_dim_region")
      spark.sql("DROP TABLE dw.dw_dim_region_orig ")
      
      LogUtils.end(task)
    } catch { case ex: Exception => LogUtils.error(task, ex) }
  }
}