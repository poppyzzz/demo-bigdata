#!/bin/bash
# $1 jar 
# $2 start_time
DATE=$(date -d"$2" +"%Y%m")
bash sparksubmit.sh com.aisino.bi.dw.fact.DwFactFpPz $1
bash sparksubmit.sh com.aisino.bi.dw.fact.DwFactFpXs $1 
bash sparksubmit.sh com.aisino.bi.dw.fact.DwFactJxfp $1
bash sparksubmit.sh com.aisino.bi.dw.fact.DwFactXxfp $1
bash sparksubmit.sh com.aisino.bi.dw.fact.DwFactJxfpMx $1
bash sparksubmit.sh com.aisino.bi.dw.fact.DwFactXxfpMx $1

bash SparkSql-f.sh /home/aisinobi/sql/bak_dw_xxfp.sql datekey=${DATE}
#作废发票
bash sparksubmit.sh com.aisino.bi.dw.fact.DwFactJxfpZf $1 ${DATE}
bash sparksubmit.sh com.aisino.bi.dw.fact.DwFactJxfpMxZf $1 ${DATE}
bash sparksubmit.sh com.aisino.bi.dw.fact.DwFactXxfpZf $1 ${DATE}
bash sparksubmit.sh com.aisino.bi.dw.fact.DwFactXxfpMxZf $1 ${DATE}
bash sparksubmit.sh com.aisino.bi.dw.fact.DwFactXxfpHz $1 ${DATE}
bash sparksubmit.sh com.aisino.bi.dw.fact.DwFactXxfpLz $1 ${DATE}

bash sparksubmit.sh com.aisino.bi.dw.fact.DwFactZffp $1

#领用指标
bash sparksubmit.sh com.aisino.bi.dw.fact.DwFactFpLy $1 ${DATE}
#抵扣发票
bash sparksubmit.sh com.aisino.bi.dw.fact.DwFactRzFpdklMx $1
#失控发票
bash sparksubmit.sh com.aisino.bi.dw.fact.DwFactRzSkfp $1

