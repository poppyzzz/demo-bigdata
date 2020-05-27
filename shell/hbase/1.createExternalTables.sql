/**
*   初始化Hive建表语句
*   huangr
*   2016-4-12
*/


CREATE EXTERNAL TABLE dw.dw_hbase_nsr (
    rowkey string,
    nsrxx map<STRING,STRING>,
    nsrkz map<STRING,STRING>,
    nsrrd map<STRING,STRING>,
    ckts map<STRING,STRING>,
    hy map<STRING,STRING>
) STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,nsrxx:,nsrkz:,nsrrd:,ckts:,hy:")
TBLPROPERTIES ("hbase.table.name" = "dw_hbase_nsr");

CREATE EXTERNAL TABLE dw.dw_hbase_jxfp(
    rowkey string,
    jxfp map<STRING,STRING>,
    jxfpqd map<STRING,STRING>
) STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,jxfp:,jxfpqd:")
TBLPROPERTIES ("hbase.table.name" = "dw_hbase_jxfp");

CREATE EXTERNAL TABLE dw.dw_hbase_xxfp(
    rowkey string,
    xxfp map<STRING,STRING>,
    xxfpqd map<STRING,STRING>
) STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,xxfp:,xxfpqd:")
TBLPROPERTIES ("hbase.table.name" = "dw_hbase_xxfp");


CREATE EXTERNAL TABLE dw.dw_hbase_jxfx_wp(
    rowkey string,
    wp map<STRING,STRING>
) STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,wp:")
TBLPROPERTIES ("hbase.table.name" = "dw_hbase_jxfx_wp");

CREATE EXTERNAL TABLE dw.dw_hbase_xxfx_wp(
    rowkey string,
    wp map<STRING,STRING>
) STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,wp:")
TBLPROPERTIES ("hbase.table.name" = "dw_hbase_xxfx_wp");



