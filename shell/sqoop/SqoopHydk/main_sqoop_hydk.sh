#!/bin/bash
# $1 begin date
# $2 end date
hdfs dfs -rm -r /user/hive/warehouse/fwsk.db/JK_YDXX

bash ${SqoopHydkPath}/sqoop_hydk_jk_ydxx.sh $1 $2
