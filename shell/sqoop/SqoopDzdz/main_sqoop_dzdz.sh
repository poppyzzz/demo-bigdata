#!/bin/bash
#$1 begin date
#s2 end date

#hadoop fs -rm -r /user/hive/warehouse/dzdz.db/DZDZ_FPXX_HYZP
#hadoop fs -rm -r /user/hive/warehouse/dzdz.db/DZDZ_FPXX_JDCFP
#hadoop fs -rm -r /user/hive/warehouse/dzdz.db/DZDZ_FPXX_PTFP

hadoop fs -rm -r /user/hive/warehouse/dzdz.db/DZDZ_FPXX_ZZSFP
#hadoop fs -rm -r /user/hive/warehouse/dzdz.db/DZDZ_FPXX_DZFP
#hadoop fs -rm -r /user/hive/warehouse/dzdz.db/DZDZ_HWXX_HYZP
#hadoop fs -rm -r /user/hive/warehouse/dzdz.db/DZDZ_HWXX_PTFP

hadoop fs -rm -r /user/hive/warehouse/dzdz.db/DZDZ_HWXX_ZZSFP
#hadoop fs -rm -r /user/hive/warehouse/dzdz.db/DZDZ_HWXX_DZFP

hadoop fs -rm -r /user/hive/warehouse/dzdz.db/DZDZ_FPXX_ZF
bash ${SqoopDzdzPath}/sqoop_dzdz_fpxx_zzsfp.sh $1 $2
# bash ${SqoopDzdzPath}/sqoop_dzdz_fpxx_jdcfp.sh $1 $2
# bash ${SqoopDzdzPath}/sqoop_dzdz_fpxx_hyzp.sh $1 $2
# bash ${SqoopDzdzPath}/sqoop_dzdz_fpxx_ptfp.sh $1 $2
# bash ${SqoopDzdzPath}/sqoop_dzdz_fpxx_dzfp.sh $1 $2
bash ${SqoopDzdzPath}/sqoop_dzdz_hwxx_zzsfp.sh $1 $2
# bash ${SqoopDzdzPath}/sqoop_dzdz_hwxx_hyzp.sh $1 $2
# bash ${SqoopDzdzPath}/sqoop_dzdz_hwxx_ptfp.sh $1 $2
# bash ${SqoopDzdzPath}/sqoop_dzdz_hwxx_dzfp.sh $1 $2
bash ${SqoopDzdzPath}/sqoop_dzdz_fpxx_zf.sh $1 $2



#DATE=$(date -d"$1" +"%Y%m")
#bash SparkSql-f.sh ${SqlPath}/back_bak_dzdz_table.sql datekey=${DATE}
