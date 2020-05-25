#!/bin/bash
sql="select * from htjs.rz_fpdkl_mx t where rz_sj between to_date('$1','yyyy-mm-dd') and to_date('$2 23:59:59','yyyy-mm-dd hh24:mi:ss') and \$CONDITIONS"
map=RZ_SJ=STRING,RZ_YF=STRING,RZ_JG=STRING,RZ_WXYY=STRING,RZ_FS=BIGINT,TASK_ID=STRING,DKYF=STRING,NSR_XZ=STRING,FP_FLAG=STRING
bash ${UtilPath}/SqoopJdbc_Yfwsk.sh ${fwsk_username} fwsk RZ_FPDKL_MX "$sql" $map

