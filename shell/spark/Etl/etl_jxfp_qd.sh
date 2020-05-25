#!/bin/bash
#$1 jar
#$2 start_time
#$3 path 
DATE=$(date -d"$2" +"%Y%m")

bash sparksubmit.sh com.aisino.bi.dlt.DltJxfpQd $1
bash sparksubmit.sh com.aisino.bi.etl.dzdz.ZzsfpJxQd $1 ${DATE}
# bash sparksubmit.sh com.aisino.bi.etl.dzdz.JdcfpJxQd $1
# bash sparksubmit.sh com.aisino.bi.etl.dzdz.HyzpJxQd $1
#bash hive-f.sh $3 insert_jxfp_qd.sql
#bash hive-f.sh $3 insert_jxfp_qd_gf.sql
#bash hive-f.sh $3 insert_jxfp_qd_xf.sql 
