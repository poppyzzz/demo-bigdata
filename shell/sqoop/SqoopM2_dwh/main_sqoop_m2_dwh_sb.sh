#!/bin/bash
# $1 begin date
# $2 end date

hadoop fs -rm -r /user/hive/warehouse/m2_dwh.db/SB_SBB
hadoop fs -rm -r /user/hive/warehouse/m2_dwh.db/SB_ZZS_YBNSR
hadoop fs -rm -r /user/hive/warehouse/m2_dwh.db/SB_ZZS_YBNSR_FB1
hadoop fs -rm -r /user/hive/warehouse/m2_dwh.db/SB_ZZS_YBNSR_FB2

#lastMonthQ=$(date -d "1 month ago $1 " +%F)
#lastMonthZ=$(date -d "1 month ago $2 " +%F)
#bash ${SqoopM2_dwhPath}/sqoop_m2_dwh_sb_sbb.sh $lastMonthQ $lastMonthZ
#bash ${SqoopM2_dwhPath}/sqoop_m2_dwh_sb_zzs_ybnsr.sh $lastMonthQ $lastMonthZ
#bash ${SqoopM2_dwhPath}/sqoop_m2_dwh_sb_zzs_ybnsr_fb1.sh $lastMonthQ $lastMonthZ
#bash ${SqoopM2_dwhPath}/sqoop_m2_dwh_sb_zzs_ybnsr_fb2.sh $lastMonthQ $lastMonthZ 

bash ${SqoopM2_dwhPath}/sqoop_m2_dwh_sb_sbb.sh $1 $2 
bash ${SqoopM2_dwhPath}/sqoop_m2_dwh_sb_zzs_ybnsr.sh $1 $2
bash ${SqoopM2_dwhPath}/sqoop_m2_dwh_sb_zzs_ybnsr_fb1.sh $1 $2
bash ${SqoopM2_dwhPath}/sqoop_m2_dwh_sb_zzs_ybnsr_fb2.sh $1 $2
