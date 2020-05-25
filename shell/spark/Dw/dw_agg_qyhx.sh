#!/bin/bash
# $1 jar
# $2 begin date
DATE=$(date -d"$2" +"%Y%m")
bash sparksubmit.sh com.aisino.bi.dw.qyhx.DwAggHyNsr $1 ${DATE}
bash sparksubmit.sh com.aisino.bi.dw.qyhx.DwAggHy $1
bash sparksubmit.sh com.aisino.bi.dw.qyhx.NsrSxygx $1 ${DATE} 
