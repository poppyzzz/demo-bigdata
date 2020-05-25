#!/bin/bash
sql="select * from htjs.rz_skfp_djb t where SKDATE between to_date('$1','yyyy-mm-dd') and to_date('$2 23:59:59','yyyy-mm-dd hh24:mi:ss') and \$CONDITIONS"
map=SKDATE=STRING,SKYF=BIGINT
bash ${UtilPath}/SqoopJdbc_Yfwsk.sh ${fwsk_username} fwsk RZ_SKFP_DJB "$sql" $map

