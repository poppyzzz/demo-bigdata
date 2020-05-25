#!/bin/bash

sql="select * from sb_zzs_ybnsr_fb2_bqjxsemx where \$CONDITIONS "
map=LRRQ=STRING,XGRQ=STRING,SJTB_SJ=STRING
bash ${UtilPath}/SqoopJdbc_Dzdz_14.sh ${dzdz_14_username} m2_dwh SB_ZZS_YBNSR_FB2_BQJXSEMX_ALL "$sql" $map
