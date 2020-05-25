#!/bin/bash

hadoop fs -rm -r /user/hive/warehouse/m2_dwh.db/DM_FRZJLX
hadoop fs -rm -r /user/hive/warehouse/m2_dwh.db/DM_GY_SWRY
hadoop fs -rm -r /user/hive/warehouse/m2_dwh.db/DM_HY
hadoop fs -rm -r /user/hive/warehouse/m2_dwh.db/DM_HYDL
hadoop fs -rm -r /user/hive/warehouse/m2_dwh.db/DM_HYML
hadoop fs -rm -r /user/hive/warehouse/m2_dwh.db/DM_HYMX
hadoop fs -rm -r /user/hive/warehouse/m2_dwh.db/DM_SWJG
hadoop fs -rm -r /user/hive/warehouse/m2_dwh.db/FP_PZ
hadoop fs -rm -r /user/hive/warehouse/m2_dwh.db/FP_XS

bash ${SqoopM2_dwhPath}/sqoop_m2_dwh_dm_frzjlx.sh
bash ${SqoopM2_dwhPath}/sqoop_m2_dwh_dm_hymx.sh
bash ${SqoopM2_dwhPath}/sqoop_m2_dwh_dm_hy.sh
bash ${SqoopM2_dwhPath}/sqoop_m2_dwh_dm_hydl.sh
bash ${SqoopM2_dwhPath}/sqoop_m2_dwh_dm_hyml.sh
bash ${SqoopM2_dwhPath}/sqoop_m2_dwh_dm_swjg.sh
bash ${SqoopM2_dwhPath}/sqoop_m2_dwh_fp_pz.sh
bash ${SqoopM2_dwhPath}/sqoop_m2_dwh_fp_xs.sh
