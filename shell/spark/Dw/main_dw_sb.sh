#!/bin/bash 
# $1: jar
# $2: begin date
# $3: end date
# $4: path
date
dateKey=$(date -d"$2" +"%Y%m")
lastMonth=$(date -d "1 month ago $2 " +%F)
lastMonthDateKey=$(date -d"$lastMonth" +"%Y%m")
echo "dateKey=${dateKey} ,lastMonth=${lastMonth} ,lastMonthDateKey=${lastMonthDateKey} "

date
bash ${SqoopM2_dwhPath}/main_sqoop_m2_dwh_sb.sh $2 $3

echo "===etl_sb================================="
date
bash ${EtlPath}/main_etl_sb.sh $1
#
echo "===dw_sb================================="
date
bash ${DwPath}/dw_sb.sh $1

echo "===bak_dw_sb================================="
date
bash SparkSql-f.sh ${SqlPath}/bak_m2_dwh_sb.sql datekey=${dateKey}
bash SparkSql-f.sh ${SqlPath}/bak_dw_sb_table.sql datekey=${dateKey}

echo "===dw_sb================================="
date
bash sparksubmit.sh com.aisino.bi.dw.jk.DwYjXbsmfxqyyjMid $1 ${dateKey}
bash sparksubmit.sh com.aisino.bi.dw.jk.DwYjXbsmfxqyyj $1 ${dateKey}

echo "===export_sb============================"
sql="delete from dw.dw_yj_xbsmfxqyyj t where t.date_key=${dateKey}"
bash sparksubmit.sh com.aisino.bi.util.ExecuteSQL $1 deleteYjXbsmfxqyyjTask "${sql}"

bash ${UtilPath}/SqoopExport.sh $1 dw dw_yj_xbsmfxqyyj
echo "===end================================="

date
