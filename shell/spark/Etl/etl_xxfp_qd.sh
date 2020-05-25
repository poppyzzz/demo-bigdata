#!/bin/bash
#$1 jar
#$2 start_time
#$3 path

DATE=$(date -d"$2" +"%Y%m")
bash sparksubmit.sh com.aisino.bi.dlt.DltXxfpQd $1
bash sparksubmit.sh com.aisino.bi.etl.dzdz.ZzsfpXxQd $1 ${DATE}
# bash sparksubmit.sh com.aisino.bi.etl.dzdz.JdcfpXxQd $1
# bash sparksubmit.sh com.aisino.bi.etl.dzdz.HyzpXxQd $1
# bash sparksubmit.sh com.aisino.bi.etl.dzdz.PtfpXxQd $1
# bash sparksubmit.sh com.aisino.bi.etl.dzdz.DzfpXxQd $1
#bash hive-f.sh $3 insert_xxfp_qd.sql
#bash hive-f.sh $3 insert_xxfp_qd_gf.sql
#bash hive-f.sh $3 insert_xxfp_qd_xf.sql 
