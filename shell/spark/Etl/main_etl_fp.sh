#!/bin/bash
#$1:jar
#$2:date
#$3:path
bash ${EtlPath}/etl_jxfp.sh $1 $2 $3
bash ${EtlPath}/etl_jxfp_qd.sh $1 $2
bash ${EtlPath}/etl_xxfp.sh $1 $2 $3
bash hive-f.sh $3 insert_region.sql
bash hive-f.sh $3 insert_nsr_hy.sql
bash ${EtlPath}/etl_xxfp_qd.sh $1 $2
bash sparksubmit.sh com.aisino.bi.dw.dm.DwDmNsr $1

bash sparksubmit.sh com.aisino.bi.etl.dzdz.EtlZffp $1
