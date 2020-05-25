#!/bin/bash
#DATE 格式为  2016-01-01
if [ $# -lt 2 ]; then
bash sparksubmit.sh com.aisino.bi.dw.dim.DwDimNsr $1 
elif [ $# == 2 ]; then
DATE=$(date -d"$2" +%Y-%m-%d)
bash sparksubmit.sh com.aisino.bi.dw.dim.DwDimNsr $1 ${DATE}
fi
