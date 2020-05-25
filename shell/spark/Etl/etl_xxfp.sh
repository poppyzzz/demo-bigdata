#!/bin/bash
#$1 jar
#$2 start_time 
#$3 path

DATE=$(date -d"$2" +"%Y%m")
bash sparksubmit.sh com.aisino.bi.dlt.DltXxfp $1
bash sparksubmit.sh com.aisino.bi.etl.dzdz.ZzsfpXx $1 ${DATE}
# bash sparksubmit.sh com.aisino.bi.etl.dzdz.JdcfpXx $1
# bash sparksubmit.sh com.aisino.bi.etl.dzdz.HyzpXx $1
# bash sparksubmit.sh com.aisino.bi.etl.dzdz.PtfpXx $1
# bash sparksubmit.sh com.aisino.bi.etl.dzdz.DzfpXx $1
#bash hive-f.sh $2 insert_xxfp.sql
#bash hive-f.sh $2 insert_xxfp_gf.sql
#bash hive-f.sh $2 insert_xxfp_xf.sql
bash hive-f.sh $3 insert_nsrxx_xxfp_gf.sql

