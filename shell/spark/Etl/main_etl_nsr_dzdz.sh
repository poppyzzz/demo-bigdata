#!/bin/bash
#$1 jar
#$2 path

bash sparksubmit.sh com.aisino.bi.etl.jszg.EtlNsrxxZjh $1
bash sparksubmit.sh com.aisino.bi.etl.dzdz.NsrxxXgmnsrHX $1
bash sparksubmit.sh com.aisino.bi.etl.dzdz.NsrxxXgmnsrZc $1
bash sparksubmit.sh com.aisino.bi.etl.dzdz.NsrxxYbnsrHX $1
bash sparksubmit.sh com.aisino.bi.etl.dzdz.NsrxxYbnsrZc $1
bash hive-f.sh $2 insert_nsrxx.sql
# bash hive-f.sh $2 insert_nsrkz.sql
# bash hive-f.sh $2 insert_nsrrd.sql
bash hive-f.sh $2 insert_region.sql
bash hive-f.sh $2 insert_nsr_hy.sql
bash hive-f.sh $2 insert_nsr_nsrsbh.sql
bash hive-f.sh $2 sync_hbase_nsr.sql
bash sparksubmit.sh com.aisino.bi.dw.dm.DwDmNsr $1
