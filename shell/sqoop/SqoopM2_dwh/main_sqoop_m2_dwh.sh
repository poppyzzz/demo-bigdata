#!/bin/bash
# $1 begin date
# $2 end date
hadoop fs -rm -r /user/hive/warehouse/m2_dwh.db/FP_PZ
hadoop fs -rm -r /user/hive/warehouse/m2_dwh.db/FP_XS
hadoop fs -rm -r /user/hive/warehouse/m2_dwh.db/FP_LY
hadoop fs -rm -r /user/hive/warehouse/m2_dwh.db/FP_LY_MX
bash ${SqoopM2_dwhPath}/sqoop_m2_dwh_fp_pz.sh $1 $2
bash ${SqoopM2_dwhPath}/sqoop_m2_dwh_fp_xs.sh $1 $2
bash ${SqoopM2_dwhPath}/sqoop_m2_dwh_fp_ly.sh $1 $2
bash ${SqoopM2_dwhPath}/sqoop_m2_dwh_fp_ly_mx.sh $1 $2