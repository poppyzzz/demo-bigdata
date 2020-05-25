#!/bin/bash

sql="select t.* from dzdz.dzdz_fpxx_zf t where t.kprq between to_date('$1','yyyy-mm-dd') and to_date('$2 23:59:59','yyyy-mm-dd hh24:mi:ss') and \$CONDITIONS"
map=ZFRQ=STRING,SE=DOUBLE,TSRQ=STRING,KPYF=STRING,SJQFRQ=STRING,ZJQFRQ=STRING,KPRQ=STRING
bash ${UtilPath}/SqoopJdbc_Dzdz.sh ${dzdz_username} dzdz DZDZ_FPXX_ZF "$sql" $map
