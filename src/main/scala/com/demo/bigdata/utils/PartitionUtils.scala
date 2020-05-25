package com.demo.bigdata.utils

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row

/**
 * @author fangang
 */
object PartitionUtils {
  /**
   * 通过路径直接读取分区表中相应分区的数据   针对parquet分区表
   * @param sqlContext
   * @param dataBase			数据库名
   * @param table					表名
   * @param partitionStr  分区字段名
   * @param beginTime			开始时间 2017XX
   * @param endTime				结束时间 2017XX
   * @return dataFrame
   */
  def getDfByPath(spark: SparkSession, dataBase: String, table: String, partitionStr: String, beginTime: Int, endTime: Int): Dataset[Row] = {
    import spark.implicits._
    val targetDir = PropertyFile.getProperty("targetDir")
    val dwbakPath = targetDir+dataBase+".db/"
    //获取单个文件
    def getData(datekey: Int): Dataset[Row] = {
        try { return spark.read.parquet(dwbakPath+table+"/"+partitionStr+"="+datekey) } catch { case t: Throwable => null}
    }
    //尾递归
    def unionAll(data: Dataset[Row],datekey: Int):Dataset[Row]= {
      if(datekey<beginTime) data
      else {
        val dataTemp = getData(datekey)
        if(dataTemp!=null) unionAll(data.unionAll(dataTemp),datekey-1)
        else if(datekey>beginTime && dataTemp==null) unionAll(data,datekey-1)
        else data
      }
    }
    
    try {
      val data = getData(endTime)
      if(data!=null) unionAll(data, endTime-1) 
      else getDfByQuery(spark, dataBase, table, partitionStr, beginTime, endTime)
    } catch { case t: Throwable => null}
  }
  
  /**
   * 通过查询读取分区表中相应分区的数据   针对parquet分区表
   * @param sqlContext
   * @param dataBase			数据库名
   * @param table					表名
   * @param partitionStr  分区字段名
   * @param beginTime			开始时间 2017XX
   * @param endTime				结束时间 2017XX
   * @return dataFrame
   */
  def getDfByQuery(spark: SparkSession, dataBase: String, table: String, partitionStr: String, beginTime: Int, endTime: Int): Dataset[Row] = {
    val fullName=dataBase+"."+table  
    val conditions = " AND ("+partitionStr+" BETWEEN "+beginTime+" AND "+endTime+")"
    val sql = "SELECT * FROM "+fullName+" WHERE 1=1 "+conditions
    println("[INFO] com.aisino.bi.util.DataFrameUtil.getDfByQuery: Executing SQL statement: "+sql)
    spark.sql(sql).drop(partitionStr) // drop partition field
  }
/**
   * 通过路径直接读取分区表中相应分区的数据及分区编号   针对parquet分区表
   * @param sqlContext
   * @param dataBase			数据库名
   * @param table					表名
   * @param partitionStr  分区字段名
   * @param beginTime			开始时间 2017XX
   * @param endTime				结束时间 2017XX
   * @return (num,data)   获取的分区的个数和分区数据
   */
  def getDfNumByPath(spark: SparkSession, dataBase: String, table: String, partitionStr: String, beginTime: Int, endTime: Int):(Int, Dataset[Row])= {
    val targetDir = PropertyFile.getProperty("targetDir")
    val dwbakPath = targetDir+dataBase+".db/"
    //获取单个文件
    def getData(datekey: Int):Dataset[Row]= {
        try { return spark.read.parquet(dwbakPath+table+"/"+partitionStr+"="+datekey) } catch { case t: Throwable => null}
    }
    //尾递归
    def unionAll(data: Dataset[Row],datekey: Int, num:Int):(Int, Dataset[Row])= {
      if(datekey<beginTime) (num ,data)
      else {
        val dataTemp = getData(datekey)
        if(dataTemp!=null) unionAll(data.unionAll(dataTemp),datekey-1,num+1)
        else if(datekey>beginTime && dataTemp==null) unionAll(data,datekey-1,num)
        else(num,data)
      }
    }
    
    try {
      val num = 1
      val data = getData(endTime)
      if(data!=null) unionAll(data, endTime-1, num) else (0,null)
    } catch { case t: Throwable => null}
  }
  
  /**
   * 根据数据库名，表名读取相应位置的数据   针对parquet分区表
   * @param dataRdd			数据集合
   * @param gpTable			GP数据库外部表表名
   * @return
   */
  def saveToHdfsPath(dataRdd: Dataset[Row], gpTable: String) {
    val externalDir = PropertyFile.getProperty("exportExternalDir")
    val path = externalDir+gpTable+"/"
    if(FileUtils.isExists(path)) FileUtils.remove(path)
    dataRdd.rdd.map(line=>line.mkString("\001").replace("null","" ).replace("\\","").replace("\u0000", "")).saveAsTextFile(path)
  }
  
  /**
   * Write data to hdfs
   * @param dataRdd		dataSet
   * @param dirName		dirName
   * @param sep 			separator, default '\001' if it's empty
   * @return Unit
   */

  def saveToHdfsPath(dataRdd: Dataset[Row], dirName: String,sep: String="\001") {
    val externalDir = PropertyFile.getProperty("exportExternalDir")
    val path = externalDir+dirName+"/"
    if(FileUtils.isExists(path)) FileUtils.remove(path)
    dataRdd.rdd.map(line=>line.mkString("=+=+=").replace(",","，" ).replace("=+=+=",sep ).replace("null","" ).replace("\\","").replace("\u0000", "")).saveAsTextFile(path)
  }

  /**
   * Save data to hive partition table
   * @param data 		DateSet to be saved
   * @param schema  Schema of the partition table to be saved
   * @param table 	Name of the partition table to be saved
   * @param partKey Partition field of partition table
   * @param partValue Partition value of partition table
   */
  def savePartition(data:Dataset[Row], schema:String, table:String, partKey:String, partValue:Any){
    data.coalesce(200).createOrReplaceTempView("tmpTable")
    val columns = getColumns(data.sparkSession, schema, table)
    val sql = s" INSERT OVERWRITE TABLE $schema.$table PARTITION ($partKey=$partValue) "+
              s" SELECT ${columns} FROM tmpTable  "
    data.sqlContext.sql(sql)
  }
  
    /**
     * Save data to hive partition table dynamicly
     * @param data    DateSet to be saved
     * @param schema  Schema of the partition table to be saved
     * @param table   Name of the partition table to be saved
     * @param partKey  Partition field of partition table
     */
    def savePartitionDynamicly(data:Dataset[Row], schema:String, table:String, partedKey:String, partKey:String){
    data.coalesce(200).createOrReplaceTempView("tmpTable")
    val columns = getColumns(data.sparkSession, schema, table)
      data.sqlContext.sql("set hive.exec.dynamic.partition=true")
      data.sqlContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    val sql = s" INSERT OVERWRITE TABLE $schema.$table PARTITION ($partKey) "+
              s" SELECT ${columns},$partedKey AS $partKey FROM tmpTable  "
    data.sqlContext.sql(sql)
  }
  /**s
   * Save data to hive partition table
   * @param data 		DateSet to be saved
   * @param schema  Schema of the partition table to be saved
   * @param table 	Name of the partition table to be saved
   * @param partKey Partition field of partition table
   * @param partValue Partition value of partition table
   */
  def savePartition(data:Dataset[Row], schema:String, table:String, partKey:String, partValue:Any, repartitionNum: Int){
    data.repartition(repartitionNum).createOrReplaceTempView("tmpTable")
    val columns = getColumns(data.sparkSession, schema, table)
    val sql = s" INSERT OVERWRITE TABLE $schema.$table PARTITION ($partKey=$partValue) "+
              s" SELECT ${columns} FROM tmpTable  "
    data.sqlContext.sql(sql)
  }  
  
  /**
   * Get columns of partition table which have only one partition field
   * @param hc			HiveContext
   * @param schema	Schema name 
   * @param table		table name
   * @param alias		alias of table for columns
   * @return String, columns of partition table without partition field
   */
  def getColumns(spark: SparkSession, schema:String, table:String, alias: String = null)={
    import spark.implicits._
    spark.sql(s"DESC $schema.$table").map(x=> if(alias == null) x(0).toString else alias+"."+x(0).toString).collect.toList.dropRight(1).mkString(",")
  }

  /**
   * Get columns of partition table which have only one partition field
   * @param hc			HiveContext
   * @param schema	Schema name 
   * @param table		table name
   * @param alias		alias of table for columns
   * @return String, columns of partition table without partition field
   */
  def getColumnsForTmp(spark: SparkSession, tmpTable:String, alias: String = null)={
    import spark.implicits._
    spark.sql(s"DESC $tmpTable").map(x=> if(alias == null) x(0).toString else alias+"."+x(0).toString).collect.toList.mkString(",")
  }  
  
}