package com.demo.bigdata.utils

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.storage.StorageLevel

/**
 * 1.将当日数据按月分区
 * 2.for(每个分区) {
 * 2.1.将当日数据与历史数据合并
 * 2.2.覆盖对应历史数据
 * }
 * @author Fangang
 */
object BackupUtils {
  
  /**
   * update increment data to history
   * @param sc
   * @param param 
   */
  def updateDataToHistory(spark:SparkSession, param:UpdateParam):Unit = {
    
    createPartitionSql(spark, param.dataBase, param.table, param.partitionField, param.partitionType)
    partitionTable(spark, param.dataBase, param.table, param.partitionField, param.partedField, param.partitionType)
    import spark.implicits._
    val partitionKeys = spark.sql(s"select distinct ${param.partitionField} from ${param.dataBase}.${param.table}_partition order by ${param.partitionField}")
      .map(x=>x(0).toString().toInt).collect
    partitionKeys.foreach { partitionKey => 
      val incrementData = PartitionUtils.getDfByPath(spark, param.dataBase, param.table+"_partition", param.partitionField, partitionKey, partitionKey)
        .persist(StorageLevel.MEMORY_AND_DISK_SER)
      val historyData = PartitionUtils.getDfByPath(spark, param.dataBase+"_bak", param.table, param.partitionField, partitionKey, partitionKey)
        .persist(StorageLevel.MEMORY_AND_DISK_SER)
      val incAvailabe = getIncAvailableData(incrementData, historyData, param.keyField, param.condition)
        .persist(StorageLevel.MEMORY_AND_DISK_SER)
      val mergedData = mergeDataToHistory(incAvailabe, historyData, param.keyField)
      PartitionUtils.savePartition(mergedData, param.dataBase+"_bak", param.table, param.partitionField, partitionKey,param.repartitionNum)
      
      for(subtable <- param.subtables) {
        createPartitionSql(spark, param.dataBase, subtable, param.partitionField, param.partitionType)
        partitionTable(spark, param.dataBase, subtable, param.partitionField, param.partedField, param.partitionType)
        val incrementSubData = PartitionUtils.getDfByPath(spark, param.dataBase, subtable+"_partition", param.partitionField, partitionKey, partitionKey)
          .persist(StorageLevel.MEMORY_AND_DISK_SER)
        val historySubData = PartitionUtils.getDfByPath(spark, param.dataBase+"_bak", subtable, param.partitionField, partitionKey, partitionKey)
          .persist(StorageLevel.MEMORY_AND_DISK_SER)
        
        val subColumns = PartitionUtils.getColumns(spark,  param.dataBase+"_bak", subtable, "sub")
        val incAvailabeSubData = getIncAvailableSubData(incrementData, incrementSubData, param.keyField, subColumns)
          .persist(StorageLevel.MEMORY_AND_DISK_SER)
        val mergedSubData = mergeDataToHistory(incAvailabeSubData, historySubData, param.keyField)
        PartitionUtils.savePartition(mergedSubData, param.dataBase+"_bak", subtable, param.partitionField, partitionKey,param.repartitionNum)
        
        incrementSubData.unpersist
        historySubData.unpersist
        incAvailabeSubData.unpersist
      }
      
      incrementData.unpersist
      historyData.unpersist
      incAvailabe.unpersist
    }
  } 
  

  /**
   * @param incrementData
   * @param historyData
   * @param keyField
   * @param condition
   * @return available increment data
   */
  def getIncAvailableData(incrementData:Dataset[Row], historyData:Dataset[Row], keyField:String, condition:String):Dataset[Row] = {
    incrementData.createOrReplaceTempView("incrementData")
    historyData.createOrReplaceTempView("historyData")
    val spark = incrementData.sparkSession
    val columns = PartitionUtils.getColumnsForTmp(spark, "historyData", "inc")
    val substract = spark.sql(s"select ${columns} from incrementData inc left join historyData his on inc.${keyField}=his.${keyField} where his.${keyField} IS NULL")
    (
    if(condition!=null&&condition!="")
      spark.sql(s"select ${columns} from incrementData inc join historyData his on inc.${keyField}=his.${keyField} where ${condition}")
        .union(substract)
    else substract
    ).distinct()
  }
  
  /**
   * @param incrementData
   * @param historyData
   * @param keyField
   * @return available increment sub data
   */
  def getIncAvailableSubData(incrementData:Dataset[Row], incrementSubData:Dataset[Row], keyField:String, subColumns: String):Dataset[Row] = {
    incrementData.createOrReplaceTempView("incrementData");
    incrementSubData.createOrReplaceTempView("incrementSubData");
    val spark = incrementData.sparkSession
    spark.sql(s"select ${subColumns} from incrementData inc join incrementSubData sub on inc.${keyField}=sub.${keyField}").distinct()
  }
  
  /**
   * @param incrementData
   * @param historyData
   * @param keyField
   * @param condition
   * @return the data that merged the increment data and history data.
   */
  def mergeDataToHistory(incAvailabe:Dataset[Row], historyData:Dataset[Row], keyField:String):Dataset[Row] = {
    incAvailabe.createOrReplaceTempView("incAvailabe");
    historyData.createOrReplaceTempView("historyData");
    val spark = incAvailabe.sparkSession;
    val difference = spark.sql(s"select his.* from incAvailabe inc right join historyData his on inc.${keyField}=his.${keyField} where inc.${keyField} is null")
    difference.union(incAvailabe)
  }
  
  /**
   * Create partition table  
   * @param spark 		SparkSession 
   * @param database  database
   * @param table 	 table
   * @param partitionField partitionField
   * @param partitionType  partitionType
   * @param suffix 				 suffix ,"partition" with default
   */
  def createPartitionSql(spark:SparkSession, database: String, table: String, partitionField: String, partitionType: String, suffix: String = "partition"){
    import spark.implicits._
    spark.sql(s"drop table if exists ${database}.${table}_${suffix}")
    val sql = new StringBuilder
    sql.append(s"create table if not exists ${database}.${table}_${suffix}(")
    // get description by no-partition table
    val desc = spark.sql(s"desc ${database}.${table}")
    
    desc.map(x=>x(0).toString+" "+x(1).toString+",").collect.foreach(sql.append(_))
    // delete the last ','
    sql.deleteCharAt(sql.length-1)
    sql.append(s") partitioned by (${partitionField} ${partitionType}) stored as parquet")
    spark.sql(sql.toString)
  }
  
  /**
   * Dynamic partitioning of tables
   * @param hc				hc
   * @param database	database
   * @param table			table
   * @param partitionField	partitionField
   * @param partedField			partedField
   * @param partitionType		partitionType
   * @param suffix suffix
   */
  def partitionTable(spark:SparkSession, database: String, table: String, partitionField: String, partedField:String, partitionType: String, suffix: String = "partition"){
    spark.sql("set hive.exec.dynamic.partition=true")
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    spark.sql(s"insert overwrite table ${database}.${table}_${suffix} partition (${partitionField}) "+
           s"select *,${partedField} as ${partitionField} from ${database}.${table} "+
           s"distribute by ${partedField}"+
           "")
  }
}

@SerialVersionUID(1234567890L)
class UpdateParam extends Serializable{
  var partitionField: String = _
  var partitionType: String = _
  var partedField: String = _
  var keyField: String = _
  var dataBase: String = _
  var table: String = _
  var subtables: Array[String] = Array()
  var condition: String = _
  var repartitionNum: Int = 120
//  var filter: String = _
}

