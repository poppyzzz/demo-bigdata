#!/bin/bash
#$1 jar
#$2 path

bash sparksubmit.sh com.aisino.bi.etl.jszg.EtlNsrxx $1
bash sparksubmit.sh com.aisino.bi.etl.jszg.EtlNsrkz $1
bash sparksubmit.sh com.aisino.bi.etl.jszg.EtlNsrrd $1
#bash sparksubmit.sh com.aisino.bi.etl.m2_dwh.EtlCkts $1
bash hive-f.sh $2 insert_nsrxx.sql
bash hive-f.sh $2 insert_nsrkz.sql
bash hive-f.sh $2 insert_nsrrd.sql
#bash hive-f.sh $2 insert_ckts.sql
bash hive-f.sh $2 insert_region.sql
bash hive-f.sh $2 insert_nsr_hy.sql
bash hive-f.sh $2 insert_nsr_nsrsbh.sql
bash hive-f.sh $2 sync_hbase_nsr.sql
bash sparksubmit.sh com.aisino.bi.dw.dm.DwDmNsr $1
