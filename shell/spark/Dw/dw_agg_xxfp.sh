#!/bin/bash
#$1:jar
#$2:start_time
#$3:path
DATE=$(date -d"$2" +"%Y%m")
bash sparksubmit.sh com.aisino.bi.dw.agg.XxfxWp $1 ${DATE}
bash sparksubmit.sh com.aisino.bi.dw.agg.XxfxNsr $1
bash sparksubmit.sh com.aisino.bi.dw.agg.XxfxSwjg $1
bash sparksubmit.sh com.aisino.bi.dw.agg.XxfxNsrWithoutSl $1

#bash hive-f.sh $3 insert_xxfx_wp.sql
#bash hive-f.sh $3 insert_xxfx_wp_gf.sql
#bash hive-f.sh $3 insert_xxfx_wp_xf.sql
#bash hive-f.sh $3 insert_xxfx_nsr.sql
#bash hive-f.sh $3 insert_xxfx_nsr_gf.sql 
#bash hive-f.sh $3 insert_xxfx_nsr_xf.sql
