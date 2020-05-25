#!/bin/bash
sql="select * from sjb_nsrxx_skfx where fxrq between to_date('$1','yyyy-mm-dd') and to_date('$2 23:59:59','yyyy-mm-dd hh24:mi:ss')  and \$CONDITIONS"
map=DJXH=STRING
#bash ${UtilPath}/SqoopJdbc_hydk.sh hydk hydk SJB_NSRXX_SKFX "$sql" $map 
bash ${UtilPath}/SqoopJdbc_hydk.sh hydk hydk SJB_NSRXX_SKFX "$sql"
