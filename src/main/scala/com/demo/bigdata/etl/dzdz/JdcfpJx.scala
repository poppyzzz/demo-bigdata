package com.demo.bigdata.etl.dzdz

import com.demo.bigdata.utils._
/**
 * 机动车统一销售发票进入进项发票的ETL过程
 a）	获取机动车发票中购方纳税人识别号在纳税人维表DW_HBASE_NSR中存在的数据
 b）	主键JXFP_ID：FPDM+FPHM+KPRQ
 c）	发票类别FP_LB：JD
 d）	价税合计JSHJ-增值税税额ZZSSE作为金额JE
 e）	NSRSBH作为销方纳税人识别号
 f）	销货单位名称XHDWMC作为销方纳税人名称
 g）	以RZDKL_BDRQ作为认证日期RZRQ
 h）	购方/销方税务机关补00
 i）	存入当前ETL_JXFP表
 * @author hada
 */
object JdcfpJx {
  
  def main(args: Array[String]) {
    val task = LogUtils.start("jdcfpJx")
    try {
      val spark = SparkUtils.init("jdcfpJx")
      val ETLFPMAPNUM = PropertyFile.getProperty("ETLFPMAPNUM").toInt
      
      UdfRegister.fillNsr(spark)
      UdfRegister.getSwjg(spark)
      UdfRegister.cutSL(spark)
    
      spark.udf.register("getJxfpId", (fpdm:String, fphm:String, kprq:String) => 
        if(null==kprq) fpdm+"X"+fphm+"X" else fpdm+"X"+fphm+"X"+kprq)
      spark.udf.register("getDouble", (str:String)=> if(null==str) "0".toDouble else str.toDouble)
      spark.udf.register("getDoubleByMinus", (str1:String,str2:String)=> 
        if(null==str1 || null==str2) "0".toDouble else str1.toDouble-str2.toDouble)
    
     val result = spark.sql("select getJxfpId(D.FPDM,D.FPHM,D.KPRQ) JXFP_ID ,D.FPDM,D.FPHM,'JD' FP_LB,"+
       "D.JSHJ-D.ZZSSE JE, cast(cutSL(D.SLV) as double) SL,D.ZZSSE SE,fillNsr(D.NSRSBH) XF_NSRSBH,D.XHDWMC XF_NSRMC,"+
       "fillNsr(D.GFSBH) GF_NSRSBH,D.GHDW GF_NSRMC,D.KPRQ,D.KPRQ RZSJ,D.SWJG_DM,"+
       "from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') CZSJ ,NSR.SWJG_KEY GF_SWJG_DM,"+
       "getSwjg(D.XF_QXSWJG_DM,fillNsr(D.NSRSBH)) XF_SWJG_DM ,case trim(D.fpzt_dm) when '0' then 'N' when '1' then 'N' else 'Y' end ZFBZ,D.SKM,"+
       "'' SHRSBH,'' SHRMC,'' FHRSBH,'' FHRMC,'' QYD,'' SKPH,"+
       "0.0 JSHJ,'' CZCH,'' CCDW,'' YSHWXX,'' BZ "+
       " from dzdz.DZDZ_FPXX_JDCFP D "+
       " JOIN DW.DW_DM_NSR NSR ON D.GFSBH = NSR.NSR_KEY and NSR.WDBZ='1' ").repartition(ETLFPMAPNUM)
       
      DataFrameUtils.saveAppend(result, "etl", "etl_jxfp")
      LogUtils.end(task)
    } catch { case ex:Exception  => LogUtils.error(task, ex) }
  }
}