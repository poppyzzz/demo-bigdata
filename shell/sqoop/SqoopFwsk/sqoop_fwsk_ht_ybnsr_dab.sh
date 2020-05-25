#!/bin/bash
sql="select * from htjs.ht_ybnsr_dab t where \$CONDITIONS"
map=CJ_RQ=STRING
bash ${UtilPath}/SqoopJdbc_Yfwsk.sh ${fwsk_username} fwsk HT_SWJG_DMB "$sql"
