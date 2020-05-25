package com.demo.bigdata.dw.dim

import com.demo.bigdata.utils._

/**
 *  由dw_hbase_nsr抽取数据到DW_DIM_NSR
 * @author Huangr
 */
object DwDimNsr {
   def main(args: Array[String]){
   val task = LogUtils.start("DwDimNsr")
   try {
    val spark = SparkUtils.init("DwDimNsr")
    val DMNSRMAPNUM = PropertyFile.getProperty("DMNSRMAPNUM").toInt
    
    val emptyToNull = spark.udf.register("emptyToNull", (data:String)=> {if (data !=null && data != "null" && data != "") data else null})
    spark.udf.register("getZt", (ztdm:String) => 
      if (ztdm == "03"||ztdm == "3 ") "03"
      else if(ztdm == "05") "05"
      else if(ztdm == "07"||ztdm =="08") "07"
      else "99")
    val tmp = if(args.length==0) " " else {val czsj = args(0); s"where t.czsj = '$czsj' "}
    
    spark.udf.register("getTimestamp", (dateString:String) => if(dateString == null || dateString.isEmpty() || dateString =="null" || dateString =="NULL") null else DateUtils.format(DateUtils.getTime(dateString),"yyyy-MM-dd HH:mm:ss"))   
    spark.udf.register("isFpdk", (nsrsbh: String) => if(nsrsbh.size==15 && nsrsbh.substring(8,10)=="DK") "Y" else "N")
    UdfRegister.getSwjg(spark)
     
    val data = spark.sql("select t.rowkey nsr_key,t.nsrxx['nsrsbh'] nsrsbh,t.nsrxx['nsrmc'] nsrmc,"+
        "t.nsrxx['fddbrmc'] fddbr_mc,t.nsrxx['zjhm'] frzjhm,t.nsrxx['dhhm'] dhhm,t.nsrkz['cwfzrmc'] cwlxr,"+
        "getSwjg(t.swjg['swjg_key'],t.nsrxx['nsrsbh']) AS SWJG_KEY,NVL(t.hy['hymx_dm'],'9999') hymx_dm,"+
        "t.nsrkz['jyfw'] jyfw,t.nsrxx['scjydz'] scjydz,"+
        "cast(t.nsrkz['cyrs'] as double) cyrs,cast(t.nsrkz['zczb'] as double) zczb,t.nsrxx['djzclx_dm'] djzclx_dm,"+
        "getTimestamp(t.nsrxx['hzdjrq']) kydjrq,"+
        "case t.nsrrd['nsrzg_dm'] when '201' then '201' when '202' then '202' when '203' then '203' when '998' then '998' else '999' end  nsrzg_dm,"+
        "getTimestamp(t.nsrrd['rdrq']) rdrq,getTimestamp(t.nsrrd['yxq_z']) qxrq,"+
        "NVL(t.hy['hy_key'],'9999') hy_key,"+
        "t.region['region_key'] region_key,"+
        "t.nsrxx['nsrdzdah'] nsrdzdah,getZt(t.nsrxx['nsrzt_dm']) nsrzt_dm,t.nsrkz['cwfzr_zjhm'] cwfzr_zjhm ,"+
        "from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') czsj,"+
        "getTimestamp(t.nsrkz['kyslrq']) kyslrq,t.nsrkz['zfjglx_dm'] zfjglx_dm,t.nsrxx['frzjlx_dm'] frzjlx_dm, "+
        "t.nsrxx['frzjlx_mc'] frzjlx_mc, isFpdk(t.rowkey) dkbz,t.nsrkz['ygznsrlx_dm'] ygznsrlx_dm,t.nsrxx['xyjb'] xyjb "+ 
        " from dw.dw_hbase_nsr t "+tmp ).repartition(DMNSRMAPNUM)
        
      val result = data.withColumn("qxrq_new", emptyToNull(data("qxrq"))).drop("qxrq").withColumnRenamed("qxrq_new", "qxrq").
        withColumn("kyslrq_new", emptyToNull(data("kyslrq"))).drop("kyslrq").withColumnRenamed("kyslrq_new", "kyslrq")

      DataFrameUtils.saveOverwrite(result, "dw", "dw_dim_nsr")  
       LogUtils.end(task)
    } catch { case ex:Exception  => LogUtils.error(task, ex) }
   }
}