#!/bin/bash
sql="select * from jk_ydxx where scrq between to_date('$1','yyyy-mm-dd') and to_date('$2 23:59:59','yyyy-mm-dd hh24:mi:ss')  and \$CONDITIONS"
map=SCRQ=STRING,FSRQ=STRING,DJXH=STRING
bash ${UtilPath}/SqoopJdbc_hydk.sh hydk dw JK_YDXX "$sql" $map 
