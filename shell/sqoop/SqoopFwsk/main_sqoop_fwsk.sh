#!/bin/bash
# $1 begin date
# $2 end date
hdfs dfs -rm -r /user/hive/warehouse/fwsk.db/RZ_FPDKL_MX
hdfs dfs -rm -r /user/hive/warehouse/fwsk.db/RZ_SKFP_DJB

bash ${SqoopFwskPath}/sqoop_fwsk_rz_fpdkl_mx.sh $1 $2
bash ${SqoopFwskPath}/sqoop_fwsk_rz_skfp_djb.sh $1 $2
