package com.demo.bigdata.utils

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset

object DataFrameUtils {
  val targetDir = PropertyFile.getProperty("targetDir")
  
/**
 * create a table by reading the file HiveQL.xml
 * @param spark
 * @param schema
 * @param table
 */
  def createTable(spark: SparkSession, schema: String, table: String) = {
    val hql = HQLFactory.getHQL(schema, table)
    spark.sql(hql)
  }
  
/**
 * whether the table is exists.
 * @param schema
 * @param table
 * @return
 */
  def isExistsTable(schema: String, table: String) = {
    val path = targetDir+"/"+schema+"/"+table
    FileUtils.isExists(path)
  }
  
/**
 * save the result into hive by append mode.
 * @param result
 * @param table
 */
  def saveAppend(result: Dataset[Row], schema: String, table: String) = {
    if(!isExistsTable(schema, table)) createTable(result.sparkSession, schema, table)
    result.write.mode(SaveMode.Append).saveAsTable(schema+"."+table)
  }
  
/**
 * save the result into hive by overwrite mode.
 * @param result
 * @param table
 */
  def saveOverwrite(result: Dataset[Row], schema: String, table: String) = {
    if(!isExistsTable(schema, table)) createTable(result.sparkSession, schema, table)
    result.write.mode(SaveMode.Overwrite).saveAsTable(schema+"."+table)
  }
}