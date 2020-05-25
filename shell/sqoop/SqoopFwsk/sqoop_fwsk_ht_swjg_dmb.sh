#!/bin/bash
sql="select * from ht_swjg_dmb t where \$CONDITIONS"
map=CJ_RQ=STRING
bash ${UtilPath}/SqoopJdbc_Yfwsk.sh m2_dwh fwsk HT_SWJG_DMB "$sql"
