package com.demo.bigdata.utils

import org.apache.spark.sql.SparkSession
import java.text.DecimalFormat; 
import org.apache.spark.sql.Row

/**
 * @author fangang
 */
object UdfRegister {
  
  /**
   * 补填纳税人key
   * @param spark
   */
  def fillNsr(spark: SparkSession) {
      spark.udf.register("fillNsr", (nsr:String) => if(nsr==null||nsr.trim().equals("")) "X99999999999999999" else nsr)
  }
  /**
   * 补填税务机关key
   * @param spark
   */
  def fillSwjg(spark: SparkSession) {
      spark.udf.register("fillSwjg", (swjg:String) => 
        if(null==swjg||"NULL"==swjg||"null"==swjg||swjg.trim().equals("")) "X9999999999" else 
          if(swjg.length()!=11) (swjg+"000000000000").substring(0,11) else swjg
      )
      
      spark.udf.register("concatSwjg", (swjg:String,gf_nsrsbh:String) => 
        if(null==swjg||"NULL"==swjg||"null"==swjg||swjg.trim().equals("") && 
            null==gf_nsrsbh||"NULL"==gf_nsrsbh||"null"==gf_nsrsbh||gf_nsrsbh.trim().equals("")) "X9999999999" else 
              if(null==swjg||"NULL"==swjg||"null"==swjg||swjg.trim().equals("")) "1"+gf_nsrsbh.substring(1,4)+"000000" else
          if(swjg.length()!=11) (swjg+"000000000000").substring(0,11) else swjg
      )
  }
  /**
   * 补填行业key
   * @param spark
   */
  def fillHy(spark: SparkSession) {
      spark.udf.register("fillHy", (hy:String) => if(hy==null||hy.trim().equals("")) "9999" else hy)
  }
  /**
   * 
   * @param spark
   */
  def getSwjg(spark: SparkSession) {
    var swjgMap:Map[String, Row] = SwjgUtils.init(spark)
    spark.udf.register("getSwjg", (swjg:String, nsr:String) => SwjgUtils.getSwjg(swjg, nsr, swjgMap))
  }
  /**
   * @param spark
   */
  def cutSL(spark: SparkSession){
    val df_3   = new DecimalFormat("######0.000") 
    val df_2   = new DecimalFormat("######0.00")
    spark.udf.register("cutSL", (sl:Double)=> if(sl<=0.015) df_3.format(sl).toDouble else df_2.format(sl).toDouble)
  }
  
  def truncRegion(spark: SparkSession){
    spark.udf.register("truncRegion", (region:String) => region.substring(0, 4))
  }
  
  /**
   * 判断身份证号是否为外地
   */
  def isWD(spark: SparkSession){
    val ShengFenDM = PropertyFile.getProperty("ShengFenDM")
    spark.udf.register("isWD",(sfzjhm: String) => isWd(sfzjhm,ShengFenDM))
  }
  
  /**
   * 判断企业是否为新增
   */
  def isXbqy(spark: SparkSession){
    spark.udf.register("isXbqy",(datekey: String, kydjrq: String) => isXbqy(datekey, kydjrq))
  }  
  /**
   * 根据开业登记日期 和 date_key判断是否为新增
   * 新增范围 12个月
   */
  def isXbqy(datekey: String, kydjrq: String)={
    if(kydjrq == null || kydjrq.isEmpty()) "0" 
    else {
      val rangeQ = getAddMonthTime(datekey,"yyyyMM",-11)
      val rangeZ = datekey
      val rq = kydjrq.substring(0,4) + kydjrq.substring(5,7)
      println("rangeQ="+rangeQ+", rangeZ="+rangeZ+", rq="+rq)
      if (rq >= rangeQ && rq <= rangeZ) "1"
      else "0"
    }
  }
  
  
  def getAddMonthTime(dateString:String, format: String, offset: Int): String = {
    if(dateString == null || dateString.isEmpty()) "" 
    else DateUtils.format(DateUtils.addMonth(DateUtils.getTime(dateString,format), offset),"yyyyMM")   
   }
  
  /**
   * 判断身份证号码是否为外地
   * @param sfzjhm
   * @param sf
   * @return String, 若为外地返回1，若不为外地返回0，若不是身份证返回-1
   */
  def isWd(sfzjhm: String,shengFenDM: String)={
    if(!isIdCard(sfzjhm)) "-1"
    else if (sfzjhm.substring(0,2) == shengFenDM) "1"
    else "0"
  }

  /**
   * 判断字符串是否为身份证号
   * @param str: String
   * @return ture or false 
   */
  def isIdCard(str: String)={
      // 15 位 或 18 位数字, 最后一位可以为字母
      val regex = "(\\d{14}[0-9a-zA-Z])|(\\d{17}[0-9a-zA-Z])"
      if(str == null) false else str.matches(regex)
  }
  
  /**
   * 根据当前时间获取DateKey
   * @param spark
   */
  def getDateKey(spark: SparkSession) = {
    spark.udf.register("getDateKey", (dateString:String) => DateUtils.format(DateUtils.getTime(dateString),"yyyyMM").toLong)
  }
  
  /**
   * 判断是否是发票代开
   * @param spark
   */
  def isFpdk(spark: SparkSession) = {
    spark.udf.register("isFpdk", (nsrsbh: String) => if(nsrsbh.size==15 && nsrsbh.substring(8,10)=="DK") "Y" else "N")
  }
  
  /**
   * 日期格式化
   * @param spark
   */
  def getTimestamp(spark: SparkSession) = {
    spark.udf.register("getTimestamp", (dateString:String) => 
      if(dateString == null || dateString.isEmpty()) dateString 
      else DateUtils.format(DateUtils.getTime(dateString),"yyyy-MM-dd HH:mm:ss"))   
  }
}