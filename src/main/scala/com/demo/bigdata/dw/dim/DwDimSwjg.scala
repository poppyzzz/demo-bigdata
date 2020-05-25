package com.demo.bigdata.dw.dim

import com.demo.bigdata.utils._

/**
 * DW税务机关表  从etl.etl_swjg抽取数据到dw.dw_dim_swjg
 * @author Liruiming
 */
object DwDimSwjg {
  def main(args: Array[String]){
    val task = LogUtils.start("dwDimSwjg")
    try {
    val spark = SparkUtils.init("dwDimSwjg")
    val now = DateUtils.getNow
    spark.sql("drop table dw.dw_dim_swjg")
    spark.udf.register("fillZero", (swjg:String) => if(null==swjg||"NULL"==swjg||"null"==swjg) swjg else swjg+"000000")
    
    //写入国家税务总局
    val result0 = spark.sql("select t.swjg_dm swjg_key,substr(t.swjg_dm,2,6) region_key,"+
      "'00000000000' swzj_dm,'国家税务总局' swzj_mc,"+
      "'' sjgs_dm, '' sjgs_mc, '' sjgs_jc,"+
      "'' dsjgs_dm,'' dsjgs_mc, '' dsjgs_jc,"+
      "'' xqjgs_dm, '' xqjgs_mc, '' xqjgs_jc,"+
      "'' xzjgs_dm, ''  xzjgs_mc,'' xzjgs_jc,"+
      "'' zjgs_dm, '' zjgs_mc, '' zjgs_jc,"+
      "'' by1_dm, '' by1_mc, '' by1_jc,"+
      "'' by2_dm, '' by2_mc, '' by2_jc,"+
      "t.swjg_dm swjg_dm, t.swjg_mc swjg_mc, "+
      "t.swjg_jc swjg_jc, t.swjg_bz, t.jgjc_dm, from_unixtime(unix_timestamp(),'yyyy-MM-dd') czrq "+
      "from etl.etl_swjg t where t.swjg_dm='00000000000' ").repartition(1)
      
    DataFrameUtils.saveAppend(result0, "dw", "dw_dim_swjg")
    
    //写入省局税务机关
    val result1 = spark.sql("select t.swjg_dm swjg_key,substr(t.swjg_dm,2,6) region_key,"+
      "'00000000000' swzj_dm,'国家税务总局' swzj_mc,"+
      "t.swjg_dm sjgs_dm, t.swjg_mc sjgs_mc, t.swjg_jc sjgs_jc, "+
      "'' dsjgs_dm,'' dsjgs_mc, '' dsjgs_jc,"+
      "'' xqjgs_dm, '' xqjgs_mc, '' xqjgs_jc,"+
      "'' xzjgs_dm, ''  xzjgs_mc,'' xzjgs_jc,"+
      "'' zjgs_dm, '' zjgs_mc, '' zjgs_jc,"+
      "'' by1_dm, '' by1_mc, '' by1_jc,"+
      "'' by2_dm, '' by2_mc, '' by2_jc,"+
      "t.swjg_dm swjg_dm, t.swjg_mc swjg_mc, "+
      "t.swjg_jc swjg_jc, t.swjg_bz, t.jgjc_dm, from_unixtime(unix_timestamp(),'yyyy-MM-dd') czrq "+
      "from etl.etl_swjg t where t.sj_swjg_dm='00000000000' ").repartition(1)
      
    DataFrameUtils.saveAppend(result1, "dw", "dw_dim_swjg")
    
    //写入地市级税务机关
    val result2 = spark.sql("select t.swjg_dm swjg_key,substr(t.swjg_dm,2,6) region_key,"+
      "'00000000000' swzj_dm,'国家税务总局' swzj_mc,"+
      "t1.swjg_dm sjgs_dm, t1.swjg_mc sjgs_mc, t1.swjg_jc sjgs_jc,"+
      "t.swjg_dm dsjgs_dm, t.swjg_mc dsjgs_mc, t.swjg_jc  dsjgs_jc,"+
      "'' xqjgs_dm, '' xqjgs_mc, '' xqjgs_jc,"+
      "'' xzjgs_dm, ''  xzjgs_mc,'' xzjgs_jc,"+
      "'' zjgs_dm, '' zjgs_mc, '' zjgs_jc,"+
      "'' by1_dm, '' by1_mc, '' by1_jc,"+
      "'' by2_dm, '' by2_mc, '' by2_jc,"+
      "t.swjg_dm swjg_dm, t.swjg_mc swjg_mc, "+
      "t.swjg_jc  swjg_jc, t.swjg_bz, t.jgjc_dm, from_unixtime(unix_timestamp(),'yyyy-MM-dd') czrq "+
      "from etl.etl_swjg t inner join etl.etl_swjg t1 "+
      "on t.sj_swjg_dm = t1.swjg_dm where t1.sj_swjg_dm='00000000000' "+
      " order by t.swjg_dm").repartition(1)
      
    DataFrameUtils.saveAppend(result2, "dw", "dw_dim_swjg")
    
    //写入区县级税务机关
    val result3 = spark.sql("select t.swjg_dm swjg_key,substr(t.swjg_dm,2,6) region_key,"+
      "'00000000000' swzj_dm,'国家税务总局' swzj_mc,"+
      "t2.swjg_dm sjgs_dm, t2.swjg_mc sjgs_mc, t2.swjg_jc sjgs_jc,"+
      "t1.swjg_dm dsjgs_dm, t1.swjg_mc dsjgs_mc, t1.swjg_jc dsjgs_jc,"+
      "t.swjg_dm xqjgs_dm, t.swjg_mc xqjgs_mc, t.swjg_jc xqjgs_jc,"+
      "'' xzjgs_dm, ''  xzjgs_mc,'' xzjgs_jc,"+
      "'' zjgs_dm, '' zjgs_mc, '' zjgs_jc,"+
      "'' by1_dm, '' by1_mc, '' by1_jc,"+
      "'' by2_dm, '' by2_mc, '' by2_jc,"+
      "t.swjg_dm swjg_dm, t.swjg_mc swjg_mc, t.swjg_jc  swjg_jc, t.swjg_bz, t.jgjc_dm, "+
      "from_unixtime(unix_timestamp(),'yyyy-MM-dd') czrq "+
      "from etl.etl_swjg t inner join etl.etl_swjg t1 on t.sj_swjg_dm = t1.swjg_dm "+
      "inner join etl.etl_swjg t2 on t1.sj_swjg_dm = t2.swjg_dm where t2.sj_swjg_dm='00000000000' "+
      " order by t.swjg_dm").repartition(1)
      
    DataFrameUtils.saveAppend(result3, "dw", "dw_dim_swjg")
    
    //写乡镇级税务机关
    val result4 = spark.sql("select t.swjg_dm swjg_key,substr(t.swjg_dm,2,6) region_key,"+
      "'00000000000' swzj_dm,'国家税务总局' swzj_mc,"+
      "t3.swjg_dm sjgs_dm, t3.swjg_mc sjgs_mc, t3.swjg_jc sjgs_jc,"+
      "t2.swjg_dm dsjgs_dm, t2.swjg_mc dsjgs_mc, t2.swjg_jc dsjgs_jc,"+
      "t1.swjg_dm xqjgs_dm, t1.swjg_mc xqjgs_mc, t1.swjg_jc xqjgs_jc,"+
      "t.swjg_dm xzjgs_dm, t.swjg_mc xzjgs_mc, t.swjg_jc xzjgs_jc,"+
      "'' zjgs_dm, '' zjgs_mc, '' zjgs_jc,"+
      "'' by1_dm, '' by1_mc, '' by1_jc,"+
      "'' by2_dm, '' by2_mc, '' by2_jc,"+
      "t.swjg_dm swjg_dm, t.swjg_mc swjg_mc, t.swjg_jc  swjg_jc, t.swjg_bz, t.jgjc_dm, "+
      "from_unixtime(unix_timestamp(),'yyyy-MM-dd') czrq "+
      " from etl.etl_swjg t "+
      "inner join etl.etl_swjg t1 on t.sj_swjg_dm = t1.swjg_dm inner join etl.etl_swjg t2 on t1.sj_swjg_dm = t2.swjg_dm "+
      "inner join etl.etl_swjg t3 on t2.sj_swjg_dm = t3.swjg_dm where t3.sj_swjg_dm='00000000000' "+
      " order by t.swjg_dm").repartition(1)
      
    DataFrameUtils.saveAppend(result4, "dw", "dw_dim_swjg")
    
    //写入组级税务机构
    val result5 = spark.sql("select t.swjg_dm swjg_key,substr(t.swjg_dm,2,6) region_key,"+
      "'00000000000' swzj_dm,'国家税务总局' swzj_mc,"+
      "t4.swjg_dm sjgs_dm, t4.swjg_mc sjgs_mc, t4.swjg_jc sjgs_jc,"+
      "t3.swjg_dm dsjgs_dm, t3.swjg_mc dsjgs_mc, t3.swjg_jc dsjgs_jc,"+
      "t2.swjg_dm xqjgs_dm, t2.swjg_mc xqjgs_mc, t2.swjg_jc xqjgs_jc,"+
      "t1.swjg_dm xzjgs_dm, t1.swjg_mc xzjgs_mc, t1.swjg_jc xzjgs_jc,"+
      "t.swjg_dm zjgs_dm, t.swjg_mc zjgs_mc, t.swjg_jc zjgs_jc,"+
      "'' by1_dm, '' by1_mc, '' by1_jc,"+
      "'' by2_dm, '' by2_mc, '' by2_jc,"+
      "t.swjg_dm swjg_dm, t.swjg_mc swjg_mc, t.swjg_jc  swjg_jc, t.swjg_bz, t.jgjc_dm, "+
      "from_unixtime(unix_timestamp(),'yyyy-MM-dd') czrq "+
      "from etl.etl_swjg t inner join etl.etl_swjg t1 on t.sj_swjg_dm = t1.swjg_dm "+
      "inner join etl.etl_swjg t2 on t1.sj_swjg_dm = t2.swjg_dm "+
      "inner join etl.etl_swjg t3 on t2.sj_swjg_dm = t3.swjg_dm inner join etl.etl_swjg t4 "+
      "on t3.sj_swjg_dm = t4.swjg_dm where t4.sj_swjg_dm='00000000000' "+
      " order by t.swjg_dm").repartition(1)
      
    DataFrameUtils.saveAppend(result5, "dw", "dw_dim_swjg")
    
    //写入备用1级税务机构
    val result6 = spark.sql("select t.swjg_dm swjg_key,substr(t.swjg_dm,2,6) region_key,"+
      "'00000000000' swzj_dm,'国家税务总局' swzj_mc,"+
      "t5.swjg_dm sjgs_dm, t5.swjg_mc sjgs_mc, t5.swjg_jc sjgs_jc,"+
      "t4.swjg_dm dsjgs_dm, t4.swjg_mc dsjgs_mc, t4.swjg_jc dsjgs_jc,"+
      "t3.swjg_dm xqjgs_dm, t3.swjg_mc xqjgs_mc, t3.swjg_jc xqjgs_jc,"+
      "t2.swjg_dm xzjgs_dm, t2.swjg_mc xzjgs_mc, t2.swjg_jc xzjgs_jc,"+
      "t1.swjg_dm zjgs_dm, t1.swjg_mc zjgs_mc, t1.swjg_jc zjgs_jc,"+
      "t.swjg_dm by1_dm, t.swjg_mc by1_mc, t.swjg_jc by1_jc,"+
      "'' by2_dm, '' by2_mc, '' by2_jc,"+
      "t.swjg_dm swjg_dm, t.swjg_mc swjg_mc, t.swjg_jc  swjg_jc, t.swjg_bz, t.jgjc_dm, "+
      "from_unixtime(unix_timestamp(),'yyyy-MM-dd') czrq "+
      "from etl.etl_swjg t inner join etl.etl_swjg t1 on t.sj_swjg_dm = t1.swjg_dm inner join etl.etl_swjg t2 "+
      "on t1.sj_swjg_dm = t2.swjg_dm inner join etl.etl_swjg t3 on t2.sj_swjg_dm = t3.swjg_dm "+
      "inner join etl.etl_swjg t4 on t3.sj_swjg_dm = t4.swjg_dm inner join etl.etl_swjg t5 "+
      "on t4.sj_swjg_dm = t5.swjg_dm where t5.sj_swjg_dm='00000000000' "+
      " order by t.swjg_dm").repartition(1)
      
    DataFrameUtils.saveAppend(result6, "dw", "dw_dim_swjg")
    
    //写入备用2级税务机构
    val result7 = spark.sql("select t.swjg_dm swjg_key,substr(t.swjg_dm,2,6) region_key,"+
      "'00000000000' swzj_dm,'国家税务总局' swzj_mc,"+
      "t6.swjg_dm sjgs_dm, t6.swjg_mc sjgs_mc, t6.swjg_jc sjgs_jc,"+
      "t5.swjg_dm dsjgs_dm, t5.swjg_mc dsjgs_mc, t5.swjg_jc dsjgs_jc,"+
      "t4.swjg_dm xqjgs_dm, t4.swjg_mc xqjgs_mc, t4.swjg_jc xqjgs_jc,"+
      "t3.swjg_dm xzjgs_dm, t3.swjg_mc xzjgs_mc, t3.swjg_jc xzjgs_jc,"+
      "t2.swjg_dm zjgs_dm, t2.swjg_mc zjgs_mc, t2.swjg_jc zjgs_jc,"+
      "t1.swjg_dm by1_dm, t1.swjg_mc by1_mc, t1.swjg_jc by1_jc,"+
      "t.swjg_dm by2_dm, t.swjg_mc by2_mc, t.swjg_jc by2_jc,"+
      "t.swjg_dm swjg_dm, t.swjg_mc swjg_mc, t.swjg_jc  swjg_jc, t.swjg_bz, t.jgjc_dm, "+
      "from_unixtime(unix_timestamp(),'yyyy-MM-dd') czrq "+
      "from etl.etl_swjg t inner join etl.etl_swjg t1 "+
      "on t.sj_swjg_dm = t1.swjg_dm inner join etl.etl_swjg t2 "+
      "on t1.sj_swjg_dm = t2.swjg_dm inner join etl.etl_swjg t3 "+
      "on t2.sj_swjg_dm = t3.swjg_dm inner join etl.etl_swjg t4 on t3.sj_swjg_dm = t4.swjg_dm "+ 
      "inner join etl.etl_swjg t5 on t4.sj_swjg_dm = t5.swjg_dm inner join etl.etl_swjg t6 "+
      "on t5.sj_swjg_dm = t6.swjg_dm where t6.sj_swjg_dm='00000000000' "+
      " order by t.swjg_dm").repartition(1)
      
    DataFrameUtils.saveAppend(result7, "dw", "dw_dim_swjg")
    
    //写入未知税务机关
    val result8 = spark.sql("select 'X9999999999' swjg_key,'X999' region_key,"+
        "'X9999999999' swjg_dm,'未知税务机关' swjg_mc, '未知机关' swjg_jc,'' swjg_bz, '' jgjc_dm,"+
        "from_unixtime(unix_timestamp(),'yyyy-MM-dd') czrq "+
        "from etl.etl_swjg t where t.swjg_dm='00000000000' ").repartition(1)
    DataFrameUtils.saveAppend(result8, "dw", "dw_dim_swjg")
    
    
    //从dw_dim_swjg_region导入
    val result9 = spark.sql("select r.swjg_key, r.region_key, r.swzj_dm, r.swzj_mc, r.sjgs_dm, r.sjgs_mc, r.sjgs_jc, "+
      "r.dsjgs_dm, r.dsjgs_mc, r.dsjgs_jc, r.xqjgs_dm, r.xqjgs_mc, r.xqjgs_jc, r.xzjgs_dm, r.xzjgs_mc, "+
      "r.xzjgs_jc, r.zjgs_dm, r.zjgs_mc, r.zjgs_jc, r.by1_dm, r.by1_mc, r.by1_jc, r.by2_dm, r.by2_mc, r.by2_jc, "+
      "r.swjg_dm, r.swjg_mc, r.swjg_jc,'' swjg_bz, '' jgjc_dm, r.czrq "+
      " from dw.dw_dim_swjg_region r left join dw.dw_dim_swjg s on r.swjg_key = s.swjg_key "+
      " where s.swjg_key is null").repartition(1)
    DataFrameUtils.saveAppend(result9, "dw", "dw_dim_swjg")
    
    LogUtils.end(task)
    } catch { case ex:Exception  => LogUtils.error(task, ex) }
  }    
}