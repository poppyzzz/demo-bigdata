package com.demo.bigdata.utils

import java.io.File
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object SparkUtils {
  val hiveDir = PropertyFile.getProperty("exportDir")
  def init(appName: String) = {
    SparkSession
    .builder()
    .config("spark.sql.warehouse.dir", hiveDir)
    .enableHiveSupport()
    .appName(appName)
    .getOrCreate()
  }
  
  def getSparkConf(appName: String) = {
    val conf = (new SparkConf).setMaster("local").setAppName("Demo");
    new SparkContext(conf)
  }
  
  def esInit(appName: String) = {
    val es_port = PropertyFile.getProperty("es_port")
    val es_nodes = PropertyFile.getProperty("es_nodes")
    SparkSession
    .builder()
    .config("spark.sql.warehouse.dir", hiveDir)
    .config("es.nodes", es_nodes)
    .config("es_port", es_port)
    .enableHiveSupport()
    .appName(appName)
    .getOrCreate()
  }
}