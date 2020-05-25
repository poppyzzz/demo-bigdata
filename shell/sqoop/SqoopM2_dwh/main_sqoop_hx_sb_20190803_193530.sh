#!/bin/bash
hdfs dfs -rm -r /user/hive/warehouse/m2_dwh.db/SB_SBB_ALL
hdfs dfs -rm -r /user/hive/warehouse/m2_dwh.db/SB_ZZS_YBNSR_ALL
hdfs dfs -rm -r /user/hive/warehouse/m2_dwh.db/SB_ZZS_YBNSR_FB1_BQXSQKMX_ALL
hdfs dfs -rm -r /user/hive/warehouse/m2_dwh.db/SB_ZZS_YBNSR_FB2_BQJXSEMX_ALL

bash ${SqoopM2_dwhPath}/sqoop_m2_dwh_hx_sb_sbb.sh
bash ${SqoopM2_dwhPath}/sqoop_m2_dwh_hx_sb_zzs_ybnsr.sh
bash ${SqoopM2_dwhPath}/sqoop_m2_dwh_hx_sb_zzs_ybnsr_fb1_bqxsqkmx.sh
bash ${SqoopM2_dwhPath}/sqoop_m2_dwh_hx_sb_zzs_ybnsr_fb2_bqjxsemx.sh

