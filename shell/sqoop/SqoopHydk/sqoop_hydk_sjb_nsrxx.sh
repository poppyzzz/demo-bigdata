#!/bin/bash
sql="select * from sjb_nsrxx where scrq between to_date('$1','yyyy-mm-dd') and to_date('$2 23:59:59','yyyy-mm-dd hh24:mi:ss')  and \$CONDITIONS"
map=SCRQ=STRING,DJXH=STRING,NSRMC=STRING
bash ${UtilPath}/SqoopJdbc_hydk.sh hydk hydk SJB_NSRXX "$sql" $map 
