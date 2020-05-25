#!/bin/bash

sql="select * from hx_sb.sb_sbb sb where sb.yzpzzl_dm='BDA0610606' and \$CONDITIONS "

map=LRRQ=STRING,XGRQ=STRING,SJTB_SJ=STRING,SKSSQQ=STRING,SKSSQZ=STRING,SBRQ_1=STRING,TBRQ_1=STRING,SLRQ=STRING
bash ${UtilPath}/SqoopJdbc_M2_dwh.sh ${m2_dwh_username} m2_dwh SB_SBB_ALL "$sql" $map
