#!/bin/bash
hadoop fs -rm -r /user/hive/warehouse/m2_dwh.db/DJ_NSRXX
hadoop fs -rm -r /user/hive/warehouse/m2_dwh.db/DJ_NSRXX_KZ
hadoop fs -rm -r /user/hive/warehouse/m2_dwh.db/RD_NSRZG

bash sqoop_m2_dwh_dj_nsrxx.sh $1 $2
bash sqoop_m2_dwh_dj_nsrxx_kz.sh $1 $2
bash sqoop_m2_dwh_rd_nsrzg.sh $1 $2

