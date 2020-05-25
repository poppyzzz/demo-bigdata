#!/bin/bash
#$1 jar
#$2 start_time
#$3 path
DATE=$(date -d"$2" +"%Y%m")
bash sparksubmit.sh com.aisino.bi.dlt.DltJxfp $1
bash sparksubmit.sh com.aisino.bi.etl.dzdz.ZzsfpJx $1 ${DATE} 
# bash sparksubmit.sh com.aisino.bi.etl.dzdz.JdcfpJx $1
# bash sparksubmit.sh com.aisino.bi.etl.dzdz.HyfpJx $1
# bash sparksubmit.sh com.aisino.bi.etl.dzdz.PtfpJx $1

#bash hive-f.sh $2 insert_jxfp.sql
#bash hive-f.sh $2 insert_jxfp_gf.sql
#bash hive-f.sh $2 insert_jxfp_xf.sql

bash hive-f.sh $3 insert_nsrxx_jxfp_xf.sql
