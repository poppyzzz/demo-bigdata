#!/bin/bash
hadoop fs -rm -r /user/hive/warehouse/dzdz.db/DZDZ_DA_XGMNSR_HX
hadoop fs -rm -r /user/hive/warehouse/dzdz.db/DZDZ_DA_XGMNSR_ZC
hadoop fs -rm -r /user/hive/warehouse/dzdz.db/DZDZ_DA_YBNSR_HX
hadoop fs -rm -r /user/hive/warehouse/dzdz.db/DZDZ_DA_YBNSR_ZC

bash ${SqoopDzdzPath}/sqoop_dzdz_xgmnsr_hx.sh $1 $2
bash ${SqoopDzdzPath}/sqoop_dzdz_xgmnsr_zc.sh $1 $2
bash ${SqoopDzdzPath}/sqoop_dzdz_ybnsr_hx.sh $1 $2
bash ${SqoopDzdzPath}/sqoop_dzdz_ybnsr_zc.sh $1 $2
