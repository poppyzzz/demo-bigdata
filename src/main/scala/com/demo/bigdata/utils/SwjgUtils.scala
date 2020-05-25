package com.demo.bigdata.utils

import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession

/**
 * The utility of swjg
 * @author Fangang
 */
object SwjgUtils {
  val DEFAULT_SWJG_KEY = "X9999999999"
  @transient
  var swjgMap:Map[String, Row] = Map()
  /**
   * initialize the class when first using.
   * @param sqlContext
   */
  def init(spark: SparkSession):Map[String, Row] = {
    import spark.implicits._
    if(swjgMap.isEmpty) 
      swjgMap=spark.sql("select * from dw.dw_dim_swjg").map { row => (row(0).toString(), row) }.collect().toMap
    return swjgMap
  }
  
  /**
   * get swjgKey, 
   * if swjgKey is null, then get swjgKey by getting the code of zone from nsrsbh
   * if swjgKey doesn't exist in the table of DwDimSwjg, then get swjgKey by getting the code of zone from nsrsbh
   * else return the swjgKey
   * @param swjg
   * @param nsr
   * @return the swjgKey
   */
  def getSwjg(swjg: String, nsr: String, swjgMap: Map[String, Row]):String = {
    if( swjg == null || swjg.equalsIgnoreCase("null") || swjg.trim().equals("") || swjg.trim().equals(DEFAULT_SWJG_KEY)) { 
      getSwjgFromNsr(nsr, swjgMap) 
    } else {
      val swjgKey = getSwjg(swjg, swjgMap)
      if(swjgKey!=null) swjgKey
      else getSwjgFromNsr(nsr, swjgMap)
    }
  }
  
  /**
   * @param swjg
   * @return
   */
  def getSwjg(swjg: String, swjgMap: Map[String, Row]):String = {
    val swjgKey = if(swjg.length()<11) (swjg+"00000000000").substring(0, 11) else swjg //fill 0
    if(swjgMap.contains(swjgKey)) swjgKey
    else if(swjg.length() < 3) null
    else getSwjg(swjg.substring(0, swjg.length()-2), swjgMap)
  }
  
  /**
   * get swjgKey by getting the code of zone from nsrsbh
   * @param nsr
   * @return the swjgKey
   */
  def getSwjgFromNsr(nsr: String, swjgMap: Map[String, Row]):String = {
    try{
      val newNsrPrefixes = List("01","11","12","13","31","51","52","55","61","81","90","91","92","93")      
      val isNewNsr = if(newNsrPrefixes.contains(nsr.substring(0, 2))) true else false
      
      val region = if(isNewNsr) nsr.substring(2, 8) else nsr.substring(0, 6)
      val swjgKey = "1"+region+"0000"
      if(swjgMap.contains(swjgKey)) swjgKey
      else {
        val region = if(isNewNsr) nsr.substring(2, 6) else nsr.substring(0, 4)
        val swjgKey = "1"+region+"000000"
        if(swjgMap.contains(swjgKey)) swjgKey
        else{
      	  val region = if(isNewNsr) nsr.substring(2, 4) else nsr.substring(0, 2)
      		val swjgKey = "1"+region+"00000000"
      	  if(swjgMap.contains(swjgKey)) swjgKey
      	  else DEFAULT_SWJG_KEY
        }
      }
    } catch {
      case t: Throwable => DEFAULT_SWJG_KEY
    }
  }
}


