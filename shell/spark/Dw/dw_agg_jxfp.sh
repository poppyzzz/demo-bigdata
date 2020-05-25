#!/bin/bash
#$1:jar
#$2:start_time
#$3:path
DATE=$(date -d"$2" +"%Y%m")
bash sparksubmit.sh com.aisino.bi.dw.agg.JxfxWp $1 ${DATE}
bash sparksubmit.sh com.aisino.bi.dw.agg.JxfxNsr $1
bash sparksubmit.sh com.aisino.bi.dw.agg.JxfxSwjg $1
bash sparksubmit.sh com.aisino.bi.dw.agg.JxfxNsrWithoutSl $1

#bash hive-f.sh $3 insert_jxfx_wp.sql
#bash hive-f.sh $3 insert_jxfx_wp_gf.sql
#bash hive-f.sh $3 insert_jxfx_wp_xf.sql
#bash hive-f.sh $3 insert_jxfx_nsr.sql
#bash hive-f.sh $3 insert_jxfx_nsr_gf.sql
#bash hive-f.sh $3 insert_jxfx_nsr_xf.sql
