#!/bin/bash

sql="select * from hx_sb.sb_zzs_ybnsr where \$CONDITIONS"
map=LRRQ=STRING,XGRQ=STRING,SJTB_SJ=STRING
bash ${UtilPath}/SqoopJdbc_M2_dwh.sh ${m2_dwh_username} m2_dwh SB_ZZS_YBNSR_ALL "$sql" $map
